package config

import (
	"os"
	"strings"
)

// LoadDotEnv loads KEY=VALUE pairs without overwriting existing environment variables.
func LoadDotEnv(path string) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		value := strings.Trim(strings.TrimSpace(parts[1]), "\"")
		if key != "" && os.Getenv(key) == "" {
			_ = os.Setenv(key, value)
		}
	}
	return nil
}

type Config struct{ Address, DBName string }

func Load() Config {
	c := Config{Address: os.Getenv("ADDR"), DBName: os.Getenv("DB_NAME")}
	if c.Address == "" {
		c.Address = ":8080"
	}
	if c.DBName == "" {
		c.DBName = "postgres"
	}
	return c
}
func (c Config) IsValid() bool {
	return strings.TrimSpace(c.Address) != "" && strings.TrimSpace(c.DBName) != ""
}
