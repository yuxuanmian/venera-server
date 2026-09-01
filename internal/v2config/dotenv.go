package v2config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// loadLocalDotEnv loads an optional .env file from the process working
// directory. Existing process environment variables always take precedence.
func loadLocalDotEnv() error {
	return loadDotEnvFile(".env")
}

func loadDotEnvFile(path string) error {
	body, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}

	values := make(map[string]string)
	for lineNumber, line := range strings.Split(string(body), "\n") {
		key, value, ok, err := parseDotEnvLine(line)
		if err != nil {
			return fmt.Errorf("%s line %d: %w", path, lineNumber+1, err)
		}
		if !ok {
			continue
		}
		values[key] = value
	}

	for key, value := range values {
		if _, exists := os.LookupEnv(key); exists {
			continue
		}
		if err := os.Setenv(key, value); err != nil {
			return fmt.Errorf("set %s from %s: %w", key, path, err)
		}
	}
	return nil
}

func parseDotEnvLine(line string) (key, value string, ok bool, err error) {
	line = strings.TrimSpace(strings.TrimPrefix(line, "\uFEFF"))
	if line == "" || strings.HasPrefix(line, "#") {
		return "", "", false, nil
	}
	if strings.HasPrefix(line, "export ") {
		line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
	}

	rawKey, rawValue, found := strings.Cut(line, "=")
	if !found {
		return "", "", false, errors.New("expected KEY=VALUE")
	}
	key = strings.TrimSpace(rawKey)
	if !validDotEnvKey(key) {
		return "", "", false, errors.New("invalid environment variable name")
	}
	value, err = parseDotEnvValue(rawValue)
	if err != nil {
		return "", "", false, err
	}
	return key, value, true, nil
}

func validDotEnvKey(key string) bool {
	if key == "" {
		return false
	}
	for index := range len(key) {
		char := key[index]
		if index == 0 {
			if !isDotEnvLetter(char) && char != '_' {
				return false
			}
			continue
		}
		if !isDotEnvLetter(char) && (char < '0' || char > '9') && char != '_' {
			return false
		}
	}
	return true
}

func isDotEnvLetter(char byte) bool {
	return (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z')
}

func parseDotEnvValue(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil
	}

	switch raw[0] {
	case '\'', '"':
		end, err := dotEnvQuoteEnd(raw, raw[0])
		if err != nil {
			return "", err
		}
		rest := strings.TrimSpace(raw[end+1:])
		if rest != "" && !strings.HasPrefix(rest, "#") {
			return "", errors.New("unexpected characters after quoted value")
		}
		value := raw[1:end]
		if raw[0] == '"' {
			value, err = strconv.Unquote(`"` + value + `"`)
			if err != nil {
				return "", fmt.Errorf("invalid double-quoted value: %w", err)
			}
		}
		return value, nil
	default:
		return stripDotEnvComment(raw), nil
	}
}

func dotEnvQuoteEnd(raw string, quote byte) (int, error) {
	escaped := false
	for index := 1; index < len(raw); index++ {
		if quote == '"' && raw[index] == '\\' && !escaped {
			escaped = true
			continue
		}
		if raw[index] == quote && !escaped {
			return index, nil
		}
		escaped = false
	}
	return 0, errors.New("unterminated quoted value")
}

func stripDotEnvComment(value string) string {
	for index := 1; index < len(value); index++ {
		if value[index] == '#' && (value[index-1] == ' ' || value[index-1] == '\t') {
			return strings.TrimSpace(value[:index])
		}
	}
	return strings.TrimSpace(value)
}
