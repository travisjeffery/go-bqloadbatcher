package bqloadbatcher

import "sync"

type buffer struct {
	sync.RWMutex

	rm map[int]map[string]*file
	wm map[int]map[string]*file

	n int
}

func newBuffer() *buffer {
	return &buffer{
		rm: map[int]map[string]*file{},
		wm: map[int]map[string]*file{},
	}
}

func (l *buffer) shift() {
	l.Lock()

	for k, v := range l.wm[l.n] {
		if _, ok := l.rm[l.n]; !ok {
			l.rm[l.n] = map[string]*file{}
		}

		l.rm[l.n][k] = v

		delete(l.wm[l.n], k)
	}

	l.n++

	l.Unlock()
}

func (l *buffer) readers(n int) map[string]*file {
	if n < 1 {
		return nil
	}
	return l.rm[n-1]
}

func (l *buffer) remove(n int, name string) {
	l.Lock()
	defer l.Unlock()

	delete(l.rm[n], name)
}

func (l *buffer) iter() int {
	l.RLock()
	defer l.RUnlock()
	return l.n
}

func (l *buffer) writer(name string) (f *file, err error) {
	var ok bool

	l.RLock()
	f, ok = l.wm[l.n][name]
	l.RUnlock()

	if !ok {
		l.Lock()
		defer l.Unlock()

		f, err = newFile(name)

		if err != nil {
			return nil, err
		}

		if l.wm[l.n] == nil {
			l.wm[l.n] = map[string]*file{}
		}

		l.wm[l.n][name] = f
	}

	return f, nil
}

func (l *buffer) clean() {
	for _, m := range l.rm {
		for _, v := range m {
			v.Remove()
		}
	}

	for _, m := range l.wm {
		for _, v := range m {
			v.Remove()
		}
	}
}
