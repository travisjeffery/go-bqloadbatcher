package bqloadbatcher

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"os"
	"path"
	"strings"
	"sync"
)

type file struct {
	sync.RWMutex

	name      string
	datasetID string
	tableID   string

	file    *os.File
	gzip    *gzip.Writer
	encoder *json.Encoder

	bufr *bytes.Buffer
	bufw *bufio.Writer
}

func newFile(name string) (*file, error) {
	dir := path.Dir(name)
	base := path.Base(name)
	names := strings.Split(base, "-")

	os.MkdirAll(dir, 0766)

	f, err := os.OpenFile(name, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0766)
	if err != nil {
		return nil, err
	}

	bw := bufio.NewWriter(f)
	g := gzip.NewWriter(bw)
	j := json.NewEncoder(g)

	return &file{
		bufw:      bw,
		encoder:   j,
		gzip:      g,
		file:      f,
		datasetID: names[1],
		tableID:   names[2],
		name:      name,
	}, nil
}

func (f *file) Write(p []byte) error {
	f.Lock()
	defer f.Unlock()

	return f.encoder.Encode(p)
}

func (f *file) Read(p []byte) (n int, err error) {
	return f.bufr.Read(p)
}

func (f *file) Flush() error {
	err := f.gzip.Flush()
	if err != nil {
		return err
	}
	return f.bufw.Flush()
}

func (f *file) Len() int {
	return f.bufr.Len()
}

func (f *file) Close() error {
	err := f.gzip.Close()
	if err != nil {
		return err
	}
	return f.file.Close()
}

func (f *file) Remove() error {
	return os.Remove(f.file.Name())
}
