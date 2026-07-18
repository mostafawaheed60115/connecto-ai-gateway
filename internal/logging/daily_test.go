package logging

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDailyWriterCreatesUTCFile(t *testing.T) {
	dir := t.TempDir()
	w, err := NewDailyWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, err = w.Write([]byte("error test\n")); err != nil {
		t.Fatal(err)
	}
	name := filepath.Join(dir, "gateway-"+time.Now().UTC().Format("2006-01-02")+".log")
	b, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "error test\n" {
		t.Fatalf("unexpected log contents: %q", b)
	}
}
