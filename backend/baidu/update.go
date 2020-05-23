package baidu

import (
	"context"
	"errors"
	"fmt"
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
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	const (
		success = 0
		retry   = 1
		failed  = 2
	)
	path := o.fs.opt.Enc.FromStandardPath(o.Remote())
	path = addSlash(path)
	fs.Debugf(o, "Update: %s", path)
	size := src.Size()
	fs.Debugf(o, "size: %d", size)
	bufCountLimit := o.fs.opt.BufferCountLimit
	bufMemLimit := o.fs.opt.BufMemLimit
	chunkSize := o.fs.opt.ChunkSize
	baiduPcs := o.fs.baiduPcs

	var bufCount int
	chunkCount := int(math.Ceil(float64(size) / float64(chunkSize)))
	if size <= chunkSize {
		bufCount = 1
	} else if size >= bufMemLimit {
		bufCount = bufCountLimit
	} else {
		bufCount = chunkCount
	}
	fs.Debugf(o, "bufCount: %d", bufCount)

	allCancel, cancel := context.WithCancel(context.Background()) // Cannot use channel because it may be cancelled repeatedly
	cancelAll := func() {
		fs.Errorf(o, "Cancel all")
		cancel()
	}

	bytesReaders := o.fs.NewBytesReaders(bufCount)
	var cases []reflect.SelectCase
	if size >= bufMemLimit {
		cases = make([]reflect.SelectCase, bufCount+1)
		for i, bytesReader := range bytesReaders {
			cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(bytesReader.done)}
		}
		cases[bufCount] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(allCancel.Done())}
	}

	wg := sync.WaitGroup{}
	checksums := make([]string, chunkCount)
	bufIndex := 0
	chunkIndex := 0
	fulled := false
	remaining := size
	for remaining > 0 {
		var chunkSizeIn int64 // current chunk size
		if remaining > chunkSize {
			chunkSizeIn = chunkSize
		} else {
			chunkSizeIn = remaining
		}

		if bufIndex == bufCount {
			fs.Debugf(o, "fulled")
			fulled = true
		}

		if fulled {
			// select all channels
			chosen, _, _ := reflect.Select(cases) // recvOK will be true if the channel has not been closed
			bufIndex = chosen
			if bufIndex == bufCount { // allCancel.Done()
				break
			}
		}

		wg.Add(1)
		// fs.Debugf(o, "bufIndex: %d", bufIndex)
		reader := bytesReaders[bufIndex]
		err := reader.write(in)
		if err != nil {
			return err
		}
		chunkIndexIn := chunkIndex
		status := retry
		uploadTmpFileFunc := func() (string, pcserror.Error) {
			// fs.Debugf(o, "Uploading size: %d", chunkSizeIn)

			// internalOrigin/pcsfunctions/pcsupload/upload.go:
			// func (pu *PCSUpload) TmpFile(ctx context.Context, partseq int, partOffset int64, r rio.ReaderLen64) (checksum string, uperr error)
			uploadTmpFileFunc := func(uploadURL string, jar http.CookieJar) (resp *http.Response, err error) {
				client := requester.NewHTTPClient()
				client.SetHTTPSecure(true)
				client.SetCookiejar(jar)
				client.SetTimeout(0)

				mr := multipartreader.NewMultipartReader()
				mr.AddFormFile("uploadedfile", "", reader)
				err = mr.CloseMultipart()
				if err != nil {
					return
				}

				requestDone := make(chan int, 1)
				go func() {
					resp, err = client.Req(http.MethodPost, uploadURL, mr, nil)
					if err != nil {
						// maybe network error
						// fs.Logf(o, "send request error: %v", err)
						status = retry
					}

					if resp != nil {
						// fs.Debugf(o, "response: %v", resp.Status)

						switch resp.StatusCode {
						case 200:
							status = success
						// Unrecoverable error
						case 400, 401, 403, 413:
							status = failed
						default:
							status = retry
						}
					}

					requestDone <- 0
				}()

				select {
				case <-ctx.Done():
					return resp, ctx.Err()
				case <-allCancel.Done():
					return resp, allCancel.Err()
				case <-requestDone:
					if status == success {
						return resp, nil
					}
					return resp, errors.New("")
				}
			}

			return baiduPcs.UploadTmpFile(uploadTmpFileFunc)
		}

		go func() {
			defer func() {
				wg.Done()
			}()

			for i := 0; i < 5; i++ {
				checksum, pcsError := uploadTmpFileFunc()
				if pcsError != nil {
					if errors.Is(pcsError.GetError(), context.Canceled) {
						fmt.Println("errors.Is(pcsError.GetError(), context.Canceled)")
						return
					}
					fmt.Println(chunkIndexIn, pcsError.Error())
					// printPcsError(pcsError)
				}
				switch status {
				case success:
					// one fragment upload successfully
					checksums[chunkIndexIn] = checksum
					reader.done <- 0
					return
				case retry:
				case failed:
					cancelAll()
					return
				}
			}
			cancelAll()
		}()

		if errors.Is(allCancel.Err(), context.Canceled) {
			break
		}
		remaining -= chunkSizeIn
		if !fulled {
			bufIndex++
		}
		chunkIndex++
	}
	fs.Debugf(o, "wait")
	wg.Wait()
	if errors.Is(allCancel.Err(), context.Canceled) {
		return errors.New("canceled")
	}

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

		return baiduPcs.Upload(path, uploadEmptyFileFunc)
	}

	for i := 0; i < 3; i++ {
		pcsError := createEmptyFileFunc()
		if pcsError != nil {
			printPcsError(pcsError)
			if shouldReCreateSuperFile(pcsError) {
				continue
			}
			fs.Errorf(o, "create empty file error: %v", pcsError)
			return pcsError
		}
		break
	}

	// Merge file fragments
	createSuperFileFunc := func() pcserror.Error {
		return baiduPcs.UploadCreateSuperFile(false, path, checksums...)
	}

	for i := 0; i < 3; i++ {
		pcsError := createSuperFileFunc()
		if pcsError != nil {
			printPcsError(pcsError)
			if shouldReCreateSuperFile(pcsError) {
				continue
			}
			fs.Errorf(o, "create super file error: %v", pcsError)
			return pcsError
		}
		break
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
