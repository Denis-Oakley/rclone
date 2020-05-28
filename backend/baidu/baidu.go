// Package baidu provides an interface to the Baidu object storage system.
package baidu

import (
	"errors"
	"fmt"
	"io"
	"log"
	"sync"

	"github.com/iikira/BaiduPCS-Go/baidupcs/pcserror"
	"github.com/iikira/baidu-tools/tieba"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/lib/encoder"
)

const (
	fieldBduss  = "bduss"
	fieldStoken = "stoken"
)

var (
	uploadBufBytesSlice []*BufBytes
	uploadBufLock       sync.Mutex
)

func init() {
	// Register with Fs
	fs.Register(&fs.RegInfo{
		Name:        "baidu",
		Description: "Baidu Net Disk",
		NewFs:       NewFs,
		Config:      registerConfig,
		Options:     fsOptions,
	})
}

func registerConfig(name string, m configmap.Mapper) {
	bduss, ok := m.Get(fieldBduss)
	if ok {
		_, err := tieba.NewUserInfoByBDUSS(bduss)
		if err != nil {
			log.Panicf("Invalid BDUSS: %#v", err)
		}
	}
}

var fsOptions = []fs.Option{
	{
		Name: fieldBduss,
		Help: "BDUSS. Fill to login." +
			"See https://github.com/iikira/BaiduPCS-Go/wiki/%E5%85%B3%E4%BA%8E-%E8%8E%B7%E5%8F%96%E7%99%BE%E5%BA%A6-BDUSS for help",
		Required: true,
	}, {
		Name:     fieldStoken,
		Help:     "Baidu STOKEN. Fill to login. Also in cookies.",
		Required: true,
	}, {
		Name:     "max_upload_thread_count",
		Help:     "Maximum upload thread limit. The bigger the more likely to cause network congestion.",
		Default:  3,
		Advanced: true,
	}, {
		Name:     "upload_chunk_size",
		Help:     "Upload chunk size.",
		Default:  int64(5 * 2e6),
		Advanced: true,
	}, {
		Name:     config.ConfigClientID,
		Help:     "Baidu App Id.",
		Default:  266719,
		Advanced: true,
	}, {
		Name:     config.ConfigEncoding,
		Help:     config.ConfigEncodingHelp,
		Advanced: true,
		Default: encoder.EncodeZero | encoder.EncodeSlash | encoder.EncodeLtGt | encoder.EncodeDoubleQuote |
			encoder.EncodeColon | encoder.EncodeQuestion | encoder.EncodeAsterisk | encoder.EncodePipe |
			encoder.EncodeBackSlash | encoder.EncodeCrLf | encoder.EncodeDel | encoder.EncodeCtl |
			encoder.EncodeLeftSpace | encoder.EncodeLeftPeriod | encoder.EncodeLeftTilde |
			encoder.EncodeLeftCrLfHtVt | encoder.EncodeRightSpace | encoder.EncodeRightPeriod |
			encoder.EncodeRightCrLfHtVt | encoder.EncodeInvalidUtf8 | encoder.EncodeDot,
	},
}

// Options defines the configuration for this backend
type Options struct {
	Bduss                string               `config:"bduss"`
	Stoken               string               `config:"stoken"`
	MaxUploadThreadCount int                  `config:"max_upload_thread_count"`
	UploadChunkSize      int64                `config:"upload_chunk_size"`
	ClientId             string               `config:"client_id"`
	Enc                  encoder.MultiEncoder `config:"encoding"`
}

type BufBytes struct {
	b    []byte
	len  int64
	i    int64 // current read index
	done chan int
}

// -----------------------------------------------

func addSlash(path string) string {
	return fmt.Sprintf("/%s", path)
}

func removeSlash(path string) string {
	return path[1:]
}

func printPcsError(pcsError pcserror.Error) {
	fmt.Printf("%#v, %#v, %#v, %#v\n", pcsError.Error(), pcsError.GetErrType(), pcsError.GetRemoteErrCode(), pcsError.GetRemoteErrMsg())
}

// -----------------------------------------------

func (r *BufBytes) Read(p []byte) (int, error) {
	n := copy(p, r.b[r.i:r.Len()])
	// fs.Debugf(nil, "read: %d", n)
	r.i += int64(n)
	if r.i == r.len {
		r.i = 0 // prepare for next reading
		return n, io.EOF
	}
	return n, nil
}

func (r *BufBytes) Len() int64 {
	return r.len
}

func (r *BufBytes) write(in io.Reader) error {
	n, err := io.ReadFull(in, r.b)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
		return err
	}
	// fs.Debugf(nil, "written: %d", n)
	r.len = int64(n)
	r.i = 0
	return nil
}

// -----------------------------------------------

// Check the interfaces are satisfied
var (
	_ fs.Fs     = (*Fs)(nil)
	_ fs.Purger = (*Fs)(nil)
	// _ fs.Mover  = (*Fs)(nil)
	_ fs.Object = (*Object)(nil)
)
