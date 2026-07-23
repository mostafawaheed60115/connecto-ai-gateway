package logging

import (
	"os"
	"path/filepath"
	"strings"
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

func TestDailyWriterRetainsAtMostFourteenDailyLogs(t *testing.T) {
	dir := t.TempDir()
	today := time.Now().UTC().Truncate(24 * time.Hour)
	for age := 1; age <= RetentionDays+2; age++ {
		name := filepath.Join(dir, "gateway-"+today.AddDate(0, 0, -age).Format(logDateLayout)+".log")
		if err := os.WriteFile(name, []byte("old\n"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	unrelated := filepath.Join(dir, "application.log")
	if err := os.WriteFile(unrelated, []byte("keep\n"), 0600); err != nil {
		t.Fatal(err)
	}

	w, err := NewDailyWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	dailyCount := 0
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, "gateway-") || !strings.HasSuffix(name, ".log") {
			continue
		}
		dateText := strings.TrimSuffix(strings.TrimPrefix(name, "gateway-"), ".log")
		if _, err := time.Parse(logDateLayout, dateText); err == nil {
			dailyCount++
		}
	}
	if dailyCount != RetentionDays {
		t.Fatalf("daily log count = %d, want %d", dailyCount, RetentionDays)
	}
	if _, err := os.Stat(unrelated); err != nil {
		t.Fatalf("unrelated file was removed: %v", err)
	}
}
