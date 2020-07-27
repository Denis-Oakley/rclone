// Package baidu provides an interface to the Baidu object storage system.
package baidu

import (
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

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
	fsLock              sync.Mutex
	listingControl      rateControl
	deletingControl     rateControl
	creatingControl     rateControl
	pcsRefuseService    = 31034
	fileNotExists       = 31066
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
		// OK: EncodeSingleQuote EncodeBackQuote EncodeDollar EncodeHash EncodePercent
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

type ringBuffer struct {
	data   []time.Time
	length int
	front  int
	rear   int
}

type rateControl struct {
	failures     ringBuffer
	lock         sync.RWMutex
	failuresLock sync.Mutex
	interval     time.Duration
	ticker       <-chan time.Time
}

// -----------------------------------------------

func addSlash(path string) string {
	return fmt.Sprintf("/%s", path)
}

func removeSlash(path string) string {
	return path[1:]
}

func printPcsError(pcsError pcserror.Error) {
	fs.Infof(nil, "%#v, %#v, %#v, %#v", pcsError.Error(), pcsError.GetErrType(), pcsError.GetRemoteErrCode(), pcsError.GetRemoteErrMsg())
}

func isFrequencyTooHigh(err error) bool {
	if err == nil {
		return false
	}
	if pcsErr, ok := err.(pcserror.Error); ok {
		printPcsError(pcsErr)
		if pcsErr.GetErrType() == pcserror.ErrTypeNetError || pcsErr.GetRemoteErrCode() == pcsRefuseService {
			return true
		}
	} else {
		if err.Error() == "net/http: timeout awaiting response headers" {
			// EOF
			return true
		}
	}
	return false
}

// -----------------------------------------------

func (r *BufBytes) Read(p []byte) (int, error) {
	n := copy(p, r.b[r.i:r.len])
	// fs.Debugf(nil, "read: %d", n)
	r.i += int64(n)
	if r.i == r.len {
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

func (r *ringBuffer) init(len int) {
	r.data = make([]time.Time, len)
	r.length = len
}

func (r *ringBuffer) len() int {
	l := r.rear - r.front
	if l < 0 {
		return l + r.length
	}
	return l
}

func (r *ringBuffer) next(i int) int {
	if i == r.length {
		return 0
	}
	return i
}

func (r *ringBuffer) enqueue(time1 time.Time) {
	r.data[r.rear] = time1
	r.rear = r.next(r.rear)
}

func (r *ringBuffer) dequeue() {
	r.front = r.next(r.front)
}

// -----------------------------------------------

func (c *rateControl) init(interval time.Duration) {
	c.failures.init(6)
	c.interval = interval
	c.ticker = time.Tick(interval)
}

func (c *rateControl) wait() {
	c.lock.RLock()
	c.lock.RUnlock()
	<-c.ticker
}

func (c *rateControl) fail() {
	now := time.Now()
	go func() {
		c.failuresLock.Lock()
		c.failures.enqueue(now)
		if c.failures.len() >= 5 {
			if now.Sub(c.failures.data[c.failures.front]) > c.interval*6 {
				c.lock.Lock()
				time.Sleep(time.Second * 10)
				c.lock.Unlock()
			}
			now = time.Now()
			for {
				front := c.failures.front
				if front == c.failures.rear {
					break
				}
				if now.Sub(c.failures.data[front]) > c.interval*6 {
					c.failures.dequeue()
				} else {
					break
				}
			}
		}
		c.failuresLock.Unlock()
	}()
}

// -----------------------------------------------

// Check the interfaces are satisfied
var (
	_ fs.Fs     = (*Fs)(nil)
	_ fs.Purger = (*Fs)(nil)
	// _ fs.Mover  = (*Fs)(nil)
	_ fs.Object = (*Object)(nil)
)
