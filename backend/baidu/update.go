package baidu

import (
	"context"
	"errors"
	"io"
	"io/ioutil"
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
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
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
	chunkCount := int(math.Ceil(float64(size) / float64(chunkSize)))
	checksums := make([]string, chunkCount)
	wg := sync.WaitGroup{}
	cancelCtx, _cancel := context.WithCancel(context.Background())      // Cannot use channel because it may be cancelled repeatedly
	allCancelCtx, cancelAll := context.WithCancel(context.Background()) // Cannot use channel because it may be cancelled repeatedly
	cancel := func() {
		fs.Debugf(o, "Cancel all")
		_cancel()
	}
	defer func() {
		// let the goroutine below return when this function returns
		cancel()
	}()
	go func() {
		select {
		case <-ctx.Done():
			cancelAll()
		case <-cancelCtx.Done():
			cancelAll()
		}
	}()

	// create an empty file, prevent 'file does not exist'
	// internalOrigin/pcsfunctions/pcsupload/upload.go:
	// func (pu *PCSUpload) CreateSuperFile(checksumList ...string) (err error)
	createEmptyFileFunc := func() pcserror.Error {
		uploadEmptyFileFunc := func(uploadURL string, jar http.CookieJar) (resp *http.Response, err error) {
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

		return baiduPcs.Upload(pathEncoded, uploadEmptyFileFunc)
	}

	wg.Add(1)
	go func() {
		defer func() {
			wg.Done()
		}()
		for i := 1; ; i++ {
			pcsError := createEmptyFileFunc()
			if pcsError != nil {
				printPcsError(pcsError)
				fs.Errorf(o, "create empty file error: %v", pcsError)
				if i >= 3 {
					cancel()
					return
				}
				continue
			}
			break
		}
	}()

	if size > 0 {
		threadCount := o.fs.opt.MaxUploadThreadCount
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
				return allCancelCtx.Err()
			}

			bufBytes := uploadBufBytesSlice[bufIndex]
			err := bufBytes.write(in)
			if err != nil {
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
					if err != nil {
						// maybe network error
						fs.Debugf(o, "send request error: %v", err)
						status = retry
					}

					if resp != nil {
						if resp.StatusCode == 200 {
							status = success
						} else {
							if fs.Config.LogLevel >= fs.LogLevelDebug {
								var bytes []byte
								bytes, err = ioutil.ReadAll(resp.Body)
								if err != nil {
									fs.Debugf(o, "read body error: %v", err)
									return
								}
								fs.Debugf(o, "response: %v\nbody: %s", resp.Status, string(bytes))
								return
							}
							switch resp.StatusCode {
							case 400, 401, 403, 413:
								// Unrecoverable error
								status = failed
							default:
								status = retry
							}
						}
					}
					return
				}

				return baiduPcs.UploadTmpFile(uploadTmpFileFunc)
			}

			wg.Add(1)
			go func() {
				defer func() {
					wg.Done()
					bufBytes.done <- 0
				}()

				for i := 0; i < 5; i++ {
					checksum, pcsError := uploadTmpFileFunc()
					if pcsError != nil {
						if errors.Is(pcsError, context.Canceled) {
							return
						}
					}
					if status == success {
						// one fragment upload successfully
						checksums[chunkIndexIn] = checksum
						return
					}
					switch status {
					case retry:
					case failed:
						cancel()
						return
					}
				}
				cancel()
			}()

			remaining -= chunkSizeIn
			chunkIndex++
		}
	}

	wg.Wait()
	if errors.Is(cancelCtx.Err(), context.Canceled) {
		return errors.New("upload failed")
	}

	if size > 0 {
		if errors.Is(ctx.Err(), context.Canceled) {
			return ctx.Err()
		}

		// Merge file fragments
		createSuperFileFunc := func() pcserror.Error {
			return baiduPcs.UploadCreateSuperFile(false, pathEncoded, checksums...)
		}

		for i := 1; ; i++ {
			pcsError := createSuperFileFunc()
			if pcsError != nil {
				fs.Debugf(o, "create super file error: %v", pcsError)
				if i >= 3 {
					return pcsError
				}
				if shouldReCreateSuperFile(pcsError) {
					continue
				}
				return pcsError
			}
			break
		}
	}

	o.size = size
	o.modTime = src.ModTime(ctx)
	fs.Debugf(o, "upload successfully")
	return nil
}

func shouldReCreateSuperFile(pcsError pcserror.Error) bool {
	switch pcsError.GetErrType() {
	case pcserror.ErrTypeRemoteError:
		switch pcsError.GetRemoteErrCode() {
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
