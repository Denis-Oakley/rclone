package baidu

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/hash"
)

// Object describes a baidu object
type Object struct {
	fs           *Fs    // what this object is part of
	relativePath string // The remote path
	absolutePath string
	size         int64     // size of the object
	modTime      time.Time // modification time of the object
}

// Fs returns the parent Fs
func (o *Object) Fs() fs.Info {
	return o.fs
}

// Return a string version
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.absolutePath
}

// Remote returns the remote path
func (o *Object) Remote() string {
	return o.relativePath
}

// Hash returns the SHA-1 of an object returning a lowercase hex string
func (o *Object) Hash(ctx context.Context, t hash.Type) (string, error) {
	// Not supported because hash may be wrong
	return "", nil
}

// Size returns the size of an object in bytes
func (o *Object) Size() int64 {
	return o.size
}

// ModTime returns the modification time of the object
// It attempts to read the objects mtime and if that isn't present the
// LastModified returned in the http headers
func (o *Object) ModTime(ctx context.Context) time.Time {
	return o.modTime
}

// SetModTime sets the modification time of the local fs object
func (o *Object) SetModTime(ctx context.Context, modTime time.Time) error {
	fs.Debugf(o, "SetModTime: %s", modTime)
	return errors.New("setModTime not supported")
}

// Storable returns a boolean showing whether this object storable
func (o *Object) Storable() bool {
	return false
}

// Open an object for read
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (in io.ReadCloser, err error) {
	fs.Debugf(o, "Open")
	fs.Errorf(o, "Download Not Supported. There are some reasons.\n"+
		"I am currently too lazy to implement this function, you are welcome to contribute."+
		"And I am worried that supporting download will increase the possibility of Baidu blocking this interface.\n")
	return nil, errors.New("download not supported")
}

// Remove an object
func (o *Object) Remove(ctx context.Context) error {
	fs.Debugf(o, "Remove")
	path := o.fs.opt.Enc.FromStandardPath(o.absolutePath)
	pcsError := o.fs.baiduPcs.Remove(path)
	return pcsError
}
