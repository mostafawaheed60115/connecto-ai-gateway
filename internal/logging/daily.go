package logging

import (
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	// RetentionDays includes the current UTC day.
	RetentionDays = 14
	logDateLayout = "2006-01-02"
)

// DailyWriter rotates files at the UTC date boundary. It is safe for slog's
// concurrent writes, creates the log directory when needed, and retains no
// more than RetentionDays daily gateway logs.
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
	t = t.UTC()
	d := t.Format(logDateLayout)
	if d == w.date && w.file != nil {
		return nil
	}
	if w.file != nil {
		_ = w.file.Close()
	}
	if err := w.removeExpired(t); err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(w.dir, "gateway-"+d+".log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0640)
	if err != nil {
		return err
	}
	w.file = f
	w.date = d
	return nil
}

// removeExpired deletes only gateway daily logs whose date falls outside the
// retention window. Unrelated files and malformed log names are untouched.
func (w *DailyWriter) removeExpired(t time.Time) error {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return err
	}
	cutoff := t.UTC().Truncate(24*time.Hour).AddDate(0, 0, -(RetentionDays - 1))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if len(name) != len("gateway-2006-01-02.log") ||
			name[:len("gateway-")] != "gateway-" ||
			filepath.Ext(name) != ".log" {
			continue
		}
		dateText := name[len("gateway-") : len(name)-len(".log")]
		date, parseErr := time.Parse(logDateLayout, dateText)
		if parseErr != nil || !date.Before(cutoff) {
			continue
		}
		if err := os.Remove(filepath.Join(w.dir, name)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
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
