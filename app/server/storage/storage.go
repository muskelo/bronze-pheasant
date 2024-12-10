package storage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	locklib "github.com/muskelo/bronze-pheasant/lib/lock"
)

func NewStorage(workdir string, lock locklib.Lock) (*Storage, error) {
	err := os.Mkdir(workdir, 0770)
	if err != nil && !os.IsExist(err) {
		return nil, err
	}
	err = os.Mkdir(filepath.Join(workdir, "removedfiles"), 0770)
	if err != nil && !os.IsExist(err) {
		return nil, err
	}
	err = os.Mkdir(filepath.Join(workdir, "tmpfiles"), 0770)
	if err != nil && !os.IsExist(err) {
		return nil, err
	}
	err = os.Mkdir(filepath.Join(workdir, "files"), 0770)
	if err != nil && !os.IsExist(err) {
		return nil, err
	}
	for _, c := range "abcdefghijklmnopqrstuvwxyz0123456789" {
		err = os.Mkdir(filepath.Join(workdir, "files", string(c)), 0770)
		if err != nil && !os.IsExist(err) {
			return nil, err
		}
		for _, cc := range "abcdefghijklmnopqrstuvwxyz0123456789" {
			err = os.Mkdir(filepath.Join(workdir, "files", string(c), string(cc)), 0770)
			if err != nil && !os.IsExist(err) {
				return nil, err
			}
		}
	}
	return &Storage{
		workdir: workdir,
        lock: lock,
	}, nil
}

type Storage struct {
	workdir string
	mutex   sync.Mutex
	lock    locklib.Lock
}

func (s *Storage) WriteFile(uuid string, src io.Reader) (written int64, err error) {
	// prepare
	filePath := s.filePath(uuid)
	tmpfilePath := s.tmpfilePath(uuid)
	_, err = os.Stat(filePath)
	if err == nil {
		err = os.ErrExist
		return
	}

	// save to temp file
	tmpfile, err := os.OpenFile(tmpfilePath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0660)
	if err != nil {
		err = fmt.Errorf("Failed create tmp file: %v\n", err)
		return
	}
	defer tmpfile.Close()
	written, err = io.Copy(tmpfile, src)
	if err != nil {
		err = fmt.Errorf("Failed write to tmp file: %v\n", err)
		return
	}
	tmpfile.Close()

	// mv from tmpdir to datadir
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if !s.lock.IsFresh() {
		err = locklib.ErrLockExpired
		return
	}
	// prevent overwrite file if datadir
	_, err = os.Stat(filePath)
	if err == nil {
		err = os.ErrExist
		return
	}
	err = os.Rename(tmpfilePath, filePath)
	return
}

func (s *Storage) ReadFile(uuid string, dst io.Writer) error {
	f, err := os.OpenFile(s.filePath(uuid), os.O_RDONLY, 0660)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(dst, f)
	return err
}

func (s *Storage) GetFile(uuid string) (*os.File, error) {
	return os.OpenFile(s.filePath(uuid), os.O_RDONLY, 0660)
}

func (s *Storage) GetFileSize(uuid string) int64 {
	fileInfo, err := os.Stat(s.filePath(uuid))
	if err != nil || fileInfo.IsDir() {
		return 0
	} else {
		return fileInfo.Size()
	}
}

func (s *Storage) RemoveFile(uuid string) error {
	filePath := s.filePath(uuid)
	_, err := os.Stat(filePath)
	if err != nil {
		return err
	}

	removedfilePath := s.removedfilePath(uuid)
	_, err = os.Stat(removedfilePath)
	if err == nil {
		return os.ErrExist
	}

	// mv from datadir to trashdir
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if !s.lock.IsFresh() {
		return fmt.Errorf("Lock is expire")
	}
	// prevent overwrite file
	_, err = os.Stat(removedfilePath)
	if err == nil {
		return os.ErrExist
	}
	return os.Rename(filePath, removedfilePath)
}

func (s *Storage) IsFileExist(uuid string) bool {
	_, err := os.Stat(s.filePath(uuid))
	return !os.IsNotExist(err)
}

func (s *Storage) filePath(uuid string) string {
	return filepath.Join(s.workdir, "files", string(uuid[0]), string(uuid[1]), uuid)
}

func (s *Storage) tmpfilePath(uuid string) string {
	return filepath.Join(s.workdir, "tmpfiles", uuid)
}

func (s *Storage) removedfilePath(uuid string) string {
	return filepath.Join(s.workdir, "removedfiles", uuid)
}
