// Package baidu provides an interface to the Baidu object storage system.
package baidu

import (
	"errors"
	"fmt"
	"io"
	"log"

	"github.com/iikira/BaiduPCS-Go/baidupcs/pcserror"
	"github.com/iikira/baidu-tools/tieba"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/lib/encoder"
)

const (
	fieldBduss       = "bduss"
	fieldStoken      = "stoken"
	fieldChunkSize   = "chunk_size"
	fieldBufMemLimit = "buffer_memory_limit"
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
		Name:     config.ConfigClientID,
		Help:     "Baidu App Id.",
		Default:  266719,
		Advanced: true,
	}, {
		Name:     fieldBufMemLimit,
		Help:     "Download and upload buffer memory limit.",
		Default:  int64(10 * 2e6),
		Advanced: true,
	}, {
		Name:     fieldChunkSize,
		Help:     "Download and upload chunk size.",
		Default:  int64(1 * 2e6),
		Advanced: true,
	}, {
		Name:     config.ConfigEncoding,
		Help:     config.ConfigEncodingHelp,
		Advanced: true,
		Default: encoder.Display |
			encoder.EncodeBackSlash |
			encoder.EncodeRightSpace |
			encoder.EncodeInvalidUtf8,
	},
}

// Options defines the configuration for this backend
type Options struct {
	ClientId         string `config:"client_id"`
	Bduss            string `config:"bduss"`
	Stoken           string `config:"stoken"`
	BufMemLimit      int64  `config:"buffer_memory_limit"`
	ChunkSize        int64  `config:"chunk_size"`
	BufferCountLimit int
	Enc              encoder.MultiEncoder `config:"encoding"`
}

type BytesReader struct {
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

func (r *BytesReader) Read(p []byte) (int, error) {
	n := copy(p, r.b[r.i:r.Len()])
	r.i += int64(n)
	// fs.Debugf(nil, "read: %d", n)
	if r.i == r.len {
		r.i = 0 // prepare for next reading
		return n, io.EOF
	}
	return n, nil
}

func (r *BytesReader) Len() int64 {
	return r.len
}

func (r *BytesReader) write(in io.Reader) error {
	n, err := io.ReadFull(in, r.b)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
		return err
	}
	r.len = int64(n)
	// fs.Debugf(nil, "wrote: %d", n)
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
