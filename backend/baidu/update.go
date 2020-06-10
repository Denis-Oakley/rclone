package baidu

import (
	"context"
	"errors"
	"io"
	"math"
	"net/http"
	"reflect"
	"strings"
	"sync"

	"github.com/iikira/BaiduPCS-Go/baidupcs/pcserror"
	"github.com/iikira/BaiduPCS-Go/internalOrigin/pcsfunctions/pcsupload"
	"github.com/iikira/BaiduPCS-Go/requester"
	"github.com/iikira/BaiduPCS-Go/requester/multipartreader"
	"github.com/rclone/rclone/fs"
)

// Update the object with the contents of the io.Reader, modTime and size
// If existing is set then it updates the object rather than creating a new one
// The new object may have been created if an error is returned
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (returnErr error) {
	const (
		success = 0
		retry   = 1
		failed  = 2
	)
	fs.Debugf(o, "Update: %s", o.absolutePath)
	pathEncoded := o.fs.opt.Enc.FromStandardPath(o.absolutePath)
	size := src.Size()
	baiduPcs := o.fs.baiduPcs
	chunkSize := o.fs.opt.UploadChunkSize
	threadCount := o.fs.opt.MaxUploadThreadCount
	chunkCount := int(math.Ceil(float64(size) / float64(chunkSize)))

	onSuccess := func() {
		o.size = size
		o.modTime = src.ModTime(ctx)
		fs.Debugf(o, "upload successfully")
	}

	// create an empty file, prevent 'file does not exist'
	// internalOrigin/pcsfunctions/pcsupload/upload.go:
	// func (pu *PCSUpload) CreateSuperFile(checksumList ...string) (err error)
	createEmptyFileFunc := func() pcserror.Error {
		createEmptyFileFunc := func(uploadURL string, jar http.CookieJar) (resp *http.Response, err error) {
			client := requester.NewHTTPClient()
			client.SetHTTPSecure(true)
			client.SetCookiejar(jar)

			mr := multipartreader.NewMultipartReader()
			mr.AddFormFile("file", "file", &pcsupload.EmptyReaderLen64{})
			err = mr.CloseMultipart()
			if err != nil {
				return
			}

			return client.Req(http.MethodPost, uploadURL, mr, nil)
		}

		return baiduPcs.Upload(pathEncoded, createEmptyFileFunc)
	}

	if size == 0 {
		var pcsErr pcserror.Error
		for i := 0; i < 3; i++ {
			pcsErr = createEmptyFileFunc()
			if pcsErr != nil {
				fs.Infof(o, "create empty file error: %s", pcsErr.Error())
				continue
			}
			onSuccess()
			return
		}
		return pcsErr
	}

	if chunkCount == 1 {
		var cases []reflect.SelectCase
		cases = make([]reflect.SelectCase, threadCount+1)
		for i, bufBytes := range uploadBufBytesSlice {
			cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(bufBytes.done)}
		}
		cases[threadCount] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())}

		// select all channels
		bufIndex, _, _ := reflect.Select(cases) // recvOK will be true if the channel has not been closed
		if errors.Is(ctx.Err(), context.Canceled) {
			return ctx.Err()
		}

		bufBytes := uploadBufBytesSlice[bufIndex]
		defer func() {
			bufBytes.done <- 0
		}()
		err := bufBytes.write(in)
		if err != nil {
			return err
		}

		uploadFileFunc := func() pcserror.Error {
			uploadFileFunc := func(uploadURL string, jar http.CookieJar) (resp *http.Response, err error) {
				client := requester.NewHTTPClient()
				client.SetHTTPSecure(true)
				client.SetCookiejar(jar)
				mr := multipartreader.NewMultipartReader()
				mr.AddFormFile("file", "file", bufBytes)
				err = mr.CloseMultipart()
				if err != nil {
					return
				}

				return client.Req(http.MethodPost, uploadURL, mr, nil)
			}

			bufBytes.i = 0
			return baiduPcs.Upload(pathEncoded, uploadFileFunc)
		}

		var pcsErr pcserror.Error
		for i := 0; i < 3; i++ {
			pcsErr = uploadFileFunc()
			if pcsErr != nil {
				fs.Infof(o, "upload file error: %s", pcsErr.Error())
				continue
			}
			onSuccess()
			return
		}
		return pcsErr
	}

	returnErrLock := sync.Mutex{}
	checksums := make([]string, chunkCount)
	wg := sync.WaitGroup{}
	cancelCtx, _cancel := context.WithCancel(context.Background())      // Cannot use channel because it may be cancelled repeatedly
	allCancelCtx, cancelAll := context.WithCancel(context.Background()) // Cannot use channel because it may be cancelled repeatedly
	cancel := func(err error) {
		fs.Debugf(o, "Cancel all")
		returnErrLock.Lock()
		if err != nil {
			returnErr = err
		}
		returnErrLock.Unlock()
		_cancel()
	}
	defer func() {
		// let the goroutine below return when this function returns
		cancel(nil)
	}()
	go func() {
		select {
		case <-ctx.Done():
			cancel(ctx.Err())
			cancelAll()
		case <-cancelCtx.Done():
			cancelAll()
		}
	}()

	wg.Add(1)
	go func() {
		defer func() {
			wg.Done()
		}()

		var pcsErr pcserror.Error
		for i := 0; i < 3; i++ {
			pcsErr = createEmptyFileFunc()
			if pcsErr != nil {
				fs.Infof(o, "create empty file error: %s", pcsErr.Error())
				continue
			}
			return
		}
		cancel(pcsErr)
		return
	}()

	remaining := size

	var cases []reflect.SelectCase
	cases = make([]reflect.SelectCase, threadCount+1)
	for i, bufBytes := range uploadBufBytesSlice {
		cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(bufBytes.done)}
	}
	cases[threadCount] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(allCancelCtx.Done())}

	chunkIndex := 0

	for remaining > 0 {
		var chunkSizeIn int64 // current chunk size
		if remaining > chunkSize {
			chunkSizeIn = chunkSize
		} else {
			chunkSizeIn = remaining
		}

		// select all channels
		bufIndex, _, _ := reflect.Select(cases) // recvOK will be true if the channel has not been closed
		if errors.Is(allCancelCtx.Err(), context.Canceled) {
			return
		}

		bufBytes := uploadBufBytesSlice[bufIndex]
		err := bufBytes.write(in)
		if err != nil {
			bufBytes.done <- 0
			return err
		}
		chunkIndexIn := chunkIndex
		status := retry // At most one goroutine write and read

		uploadTmpFileFunc := func() (string, pcserror.Error) {
			// internalOrigin/pcsfunctions/pcsupload/upload.go:
			// func (pu *PCSUpload) TmpFile(ctx context.Context, partseq int, partOffset int64, r rio.ReaderLen64) (checksum string, uperr error)
			uploadTmpFileFunc := func(uploadURL string, jar http.CookieJar) (resp *http.Response, err error) {
				client := requester.NewHTTPClient()
				client.SetHTTPSecure(true)
				client.SetCookiejar(jar)
				client.SetTimeout(0)
				mr := multipartreader.NewMultipartReader()
				mr.AddFormFile("uploadedfile", "", bufBytes)
				err = mr.CloseMultipart()
				if err != nil {
					return
				}

				resp, err = client.Req(http.MethodPost, uploadURL, mr, nil)
				if resp != nil {
					switch resp.StatusCode {
					case 400, 401, 403, 413:
						// Unrecoverable error
						status = failed
					}
				}
				return
			}

			bufBytes.i = 0
			return baiduPcs.UploadTmpFile(uploadTmpFileFunc)
		}

		wg.Add(1)
		go func() {
			defer func() {
				wg.Done()
				bufBytes.done <- 0
			}()

			var (
				checksum string
				pcsErr   pcserror.Error
			)
			for i := 0; i < 5; i++ {
				checksum, pcsErr = uploadTmpFileFunc()
				if pcsErr != nil {
					fs.Infof(o, "upload temp file error: %s", pcsErr.Error())
					if status == failed {
						cancel(pcsErr)
						return
					}
				} else {
					// one fragment upload successfully
					checksums[chunkIndexIn] = checksum
					return
				}
			}
			cancel(pcsErr)
		}()

		remaining -= chunkSizeIn
		chunkIndex++
	}

	wg.Wait()
	if errors.Is(allCancelCtx.Err(), context.Canceled) {
		return
	}

	// Merge file fragments
	createSuperFileFunc := func() pcserror.Error {
		return baiduPcs.UploadCreateSuperFile(false, pathEncoded, checksums...)
	}

	for i := 1; ; i++ {
		pcsErr := createSuperFileFunc()
		if pcsErr != nil {
			fs.Infof(o, "create super file error: %s", pcsErr.Error())
			if i >= 3 {
				return pcsErr
			}
			if shouldReCreateSuperFile(pcsErr) {
				continue
			}
			return pcsErr
		}
		break
	}

	onSuccess()
	return nil
}

func shouldReCreateSuperFile(pcsError pcserror.Error) bool {
	switch pcsError.GetErrType() {
	case pcserror.ErrTypeRemoteError:
		switch pcsError.GetRemoteErrCode() {
		// 31352, commit superfile2 failed
		case 31363:
			// block miss in superfile2. Upload status expired
			return true
		case 31200:
			// server error
			// [Method:Insert][Error:Insert Request Forbid]
			return true
		}
	case pcserror.ErrTypeNetError:
		if strings.Contains(pcsError.GetError().Error(), "413 Request Entity Too Large") {
			// 请求实体过大
			return false
		}
	}
	return true
}
