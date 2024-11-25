package storage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

func NewStorage(datadir, tmpdir, trashdir string) (*Storage, error) {
	err := os.Mkdir(tmpdir, 0770)
	if err != nil && !os.IsExist(err) {
		return nil, err
	}
	err = os.Mkdir(trashdir, 0770)
	if err != nil && !os.IsExist(err) {
		return nil, err
	}
	err = os.Mkdir(datadir, 0770)
	if err != nil && !os.IsExist(err) {
		return nil, err
	}
	for _, c := range "abcdefghijklmnopqrstuvwxyz0123456789" {
		err = os.Mkdir(filepath.Join(datadir, string(c)), 0770)
		if err != nil && !os.IsExist(err) {
			return nil, err
		}
		for _, cc := range "abcdefghijklmnopqrstuvwxyz0123456789" {
			err = os.Mkdir(filepath.Join(datadir, string(c), string(cc)), 0770)
			if err != nil && !os.IsExist(err) {
				return nil, err
			}
		}
	}
	return &Storage{
		datadir:  datadir,
		tmpdir:   tmpdir,
		trashdir: trashdir,
	}, nil
}

type Storage struct {
	datadir  string
	tmpdir   string
	trashdir string
	mutex    sync.Mutex
}

func (s *Storage) WriteFile(uuid string, src io.Reader) error {
	// prepare
	path := s.path(uuid)
	tmppath := s.tmpPath(uuid)
	_, err := os.Stat(path)
	if err == nil {
		return os.ErrExist
	}

	// save to temp file
	tmpfile, err := os.OpenFile(tmppath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0660)
	if err != nil {
		return fmt.Errorf("Failed create tmp file: %v\n", err)
	}
	defer tmpfile.Close()
	_, err = io.Copy(tmpfile, src)
	if err != nil {
		return fmt.Errorf("Failed write to tmp file: %v\n", err)
	}
	tmpfile.Close()

	// mv from tmpdir to datadir
	s.mutex.Lock()
	defer s.mutex.Unlock()
	// prevent overwrite file if datadir
	_, err = os.Stat(path)
	if err == nil {
		return os.ErrExist
	}
	return os.Rename(tmppath, path)
}

func (s *Storage) ReadFile(uuid string, dst io.Writer) error {
	f, err := os.OpenFile(s.path(uuid), os.O_RDONLY, 0660)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(dst, f)
	return err
}

func (s *Storage) RemoveFile(uuid string) error {
	path := s.path(uuid)
	_, err := os.Stat(path)
	if err != nil {
		return err
	}

	trashpath := s.trashPath(uuid)
	_, err = os.Stat(trashpath)
	if err == nil {
		return os.ErrExist
	}

	// mv from datadir to trashdir
	s.mutex.Lock()
	defer s.mutex.Unlock()
	// prevent overwrite file in trashdir
	_, err = os.Stat(trashpath)
	if err == nil {
		return os.ErrExist
	}
	return os.Rename(path, trashpath)
}

func (s *Storage) IsFileExist(uuid string) bool {
	_, err := os.Stat(s.path(uuid))
	return !os.IsNotExist(err)
}

func (s *Storage) path(uuid string) string {
	return filepath.Join(s.datadir, string(uuid[0]), string(uuid[1]), uuid)
}

func (s *Storage) tmpPath(uuid string) string {
	return filepath.Join(s.tmpdir, uuid)
}

func (s *Storage) trashPath(uuid string) string {
	return filepath.Join(s.trashdir, uuid)
}
