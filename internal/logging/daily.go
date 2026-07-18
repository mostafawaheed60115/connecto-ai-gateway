package logging

import (
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// DailyWriter rotates files at the UTC date boundary. It is safe for slog's
// concurrent writes and creates the log directory when needed.
type DailyWriter struct {
	mu   sync.Mutex
	dir  string
	date string
	file *os.File
}

func NewDailyWriter(dir string) (*DailyWriter, error) {
	if dir == "" {
		dir = "logs"
	}
	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, err
	}
	w := &DailyWriter{dir: dir}
	if err := w.rotate(time.Now().UTC()); err != nil {
		return nil, err
	}
	return w, nil
}
func (w *DailyWriter) rotate(t time.Time) error {
	d := t.UTC().Format("2006-01-02")
	if d == w.date && w.file != nil {
		return nil
	}
	if w.file != nil {
		_ = w.file.Close()
	}
	f, err := os.OpenFile(filepath.Join(w.dir, "gateway-"+d+".log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0640)
	if err != nil {
		return err
	}
	w.file = f
	w.date = d
	return nil
}
func (w *DailyWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.rotate(time.Now().UTC()); err != nil {
		return 0, err
	}
	return w.file.Write(p)
}
func (w *DailyWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}
func Multi(w io.Writer, file io.Writer) io.Writer { return io.MultiWriter(w, file) }
