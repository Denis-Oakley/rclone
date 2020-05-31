package baidu

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"time"

	"github.com/iikira/BaiduPCS-Go/baidupcs"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/hash"
)

// Fs represents a remote baidu
type Fs struct {
	name          string // name of this remote
	root          string // the path we are working on
	rootWithSlash string
	opt           Options      // parsed options
	features      *fs.Features // optional features
	baiduPcs      *baidupcs.BaiduPCS
	// dirCache     *dircache.DirCache // Map of directory path to directory id
}

// NewFs constructs an Fs from the path, container:path
func NewFs(name, root string, m configmap.Mapper) (fs.Fs, error) {
	fs.Debugf(root, "NewFs")
	opt := new(Options)
	// Parse config into Options struct
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}

	appId, err := strconv.Atoi(opt.ClientId)
	if err != nil {
		return nil, err
	}
	baiduPcs := baidupcs.NewPCS(appId, opt.Bduss)

	baiduPcs.SetStoken(opt.Stoken)
	baiduPcs.SetPanUserAgent(baidupcs.NetdiskUA)
	baiduPcs.SetHTTPS(true)

	f := &Fs{
		name:          name,
		root:          root,
		rootWithSlash: addSlash(root),
		opt:           *opt,
		baiduPcs:      baiduPcs,
	}

	uploadBufLock.Lock()
	if len(uploadBufBytesSlice) == 0 {
		uploadBufBytesSlice = f.newBufBytesSlice(opt.MaxUploadThreadCount)
	}
	uploadBufLock.Unlock()

	f.features = (&fs.Features{
		CaseInsensitive:         true,
		CanHaveEmptyDirectories: true,
		Purge:                   f.Purge,
	}).Fill(f)

	return f, nil
}

// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string {
	return f.name
}

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string {
	return f.root
}

// String converts this Fs to a string
func (f *Fs) String() string {
	return fmt.Sprintf("Backend for Baidu Net Disk, root: %s", f.rootWithSlash)
}

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features {
	return f.features
}

// Precision return the precision of this Fs
func (f *Fs) Precision() time.Duration {
	return time.Second
}

// NewObject finds the Object at remote.  If it can't be found
// it returns the error fs.ErrorObjectNotFound.
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	absolutePath := filepath.Join(f.rootWithSlash, remote)
	fs.Debugf(f, "newObjectWithInfo: %s", absolutePath)
	fullPathEncoded := f.opt.Enc.FromStandardPath(absolutePath)
	meta, err := f.baiduPcs.FilesDirectoriesMeta(fullPathEncoded)
	if err != nil {
		if err.GetRemoteErrCode() == 31066 {
			// File not found
			return nil, fs.ErrorObjectNotFound
		}
		return nil, err
	}
	if meta.Isdir {
		return nil, fs.ErrorNotAFile
	}
	return &Object{
		fs:           f,
		relativePath: remote,
		absolutePath: absolutePath,
		size:         meta.Size,
		modTime:      time.Unix(meta.Mtime, 0),
	}, nil
}

// List the objects and directories in dir into entries.  The
// entries can be returned in any order but should be for a
// complete directory.
// dir should be "" to list the root, and should not have
// trailing slashes.
// This should return ErrDirNotFound if the directory isn't
// found.
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	absolutePath := filepath.Join(f.rootWithSlash, dir)
	fs.Debugf(f, "List: %s", absolutePath)
	dirEncoded := f.opt.Enc.FromStandardPath(absolutePath)
	list, err := f.baiduPcs.FilesDirectoriesList(dirEncoded, nil)
	if err != nil {
		return nil, err
	}

	dirEntries := make([]fs.DirEntry, 0, len(list))
	for _, entry := range list {
		relativePath := filepath.Join(dir, f.opt.Enc.ToStandardName(entry.Filename))
		if entry.Isdir {
			newDir := fs.NewDir(relativePath, time.Unix(entry.Mtime, 0))
			dirEntries = append(dirEntries, newDir)
		} else {
			dirEntries = append(dirEntries, &Object{
				fs:           f,
				relativePath: relativePath,
				absolutePath: entry.Path,
				size:         entry.Size,
				modTime:      time.Unix(entry.Mtime, 0),
			})
		}
	}
	return dirEntries, nil
}

// Put the object
// Copy the reader in to the new object which is returned
// The new object may have been created if an error is returned
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	o := &Object{
		fs:           f,
		relativePath: src.Remote(),
		absolutePath: filepath.Join(f.rootWithSlash, src.Remote()),
	}
	fs.Debugf(f, "Put: %s", o.absolutePath)
	err := o.Update(ctx, in, src, options...)
	return o, err
}

// PutStream uploads to the remote path with the modTime given of indeterminate size
// func (f *Fs) PutStream(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
//     printDebug("PutStream")
//     return f.Put(ctx, in, src, options...)
// }

// Mkdir creates the container if it doesn't exist
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	dir = filepath.Join(f.rootWithSlash, dir)
	fs.Debugf(f, "Mkdir: %s", dir)
	path := f.opt.Enc.FromStandardPath(dir)
	pcsError := f.baiduPcs.Mkdir(path)
	return pcsError
}

// Rmdir deletes the root folder
// Returns an error if it isn't empty
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	dir = filepath.Join(f.rootWithSlash, dir)
	fs.Debugf(f, "Rmdir: %s", dir)
	path := f.opt.Enc.FromStandardPath(dir)
	pcsError := f.baiduPcs.Remove(path)
	return pcsError
}

// Purge deletes all the files and the container
// Optional interface: Only implement this if you have a way of
// deleting all the files quicker than just running Remove() on the
// result of List()
func (f *Fs) Purge(ctx context.Context) error {
	fs.Debugf(f, "Purge")
	path := f.opt.Enc.FromStandardPath(f.rootWithSlash)
	pcsError := f.baiduPcs.Remove(path)
	return pcsError
}

// Move src to this remote using server side move operations.
// This is stored with the remote path given
// It returns the destination Object and a possible error
// Will only be called if src.Fs().Name() == f.Name()
// If it isn't possible then return fs.ErrorCantMove
// func (f *Fs) Move(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
//     fs.Debugf(f, "Move: %s -> %s", src.Remote(), remote)
//     return &Object{
//         fs:      f,
//         remote:  remote,
//         size:    src.Size(),
//         modTime: time.Now(),
//     }, nil
// }

// Hashes returns the supported hash sets.
func (f *Fs) Hashes() hash.Set {
	return hash.Set(hash.MD5)
}

func (f *Fs) newBufBytesSlice(count int) []*BufBytes {
	bufBytesSlice := make([]*BufBytes, count)
	for i := 0; i < count; i++ {
		bufBytesSlice[i] = f.newBufBytes()
	}
	return bufBytesSlice
}

func (f *Fs) newBufBytes() *BufBytes {
	bufBytes := new(BufBytes)
	bufBytes.b = make([]byte, f.opt.UploadChunkSize)
	bufBytes.done = make(chan int, 1)
	bufBytes.done <- 0
	return bufBytes
}
