package interval

import (
	"fmt"
	"time"
)

// IntervalForAge returns the next scan interval according to the v3.5 lookup table.
func IntervalForAge(age time.Duration) time.Duration {
	days := age.Hours() / 24
	switch {
	case days <= 14:
		return 24 * time.Hour
	case days <= 59:
		return 2 * 24 * time.Hour
	case days <= 119:
		return 3 * 24 * time.Hour
	case days <= 194:
		return 4 * 24 * time.Hour
	case days <= 269:
		return 5 * 24 * time.Hour
	case days <= 365:
		return 7 * 24 * time.Hour
	case days <= 730:
		return 8 * 24 * time.Hour
	default:
		return 14 * 24 * time.Hour
	}
}

// NextDue computes the next due time after a scan.
// lastUpdate may be empty (fallback 24h), "YYYY-MM-DD", "YYYY-MM-DD HH:MM:SS", or RFC3339.
func NextDue(scanTime time.Time, lastUpdate string) (time.Time, error) {
	if lastUpdate == "" {
		return scanTime.Add(24 * time.Hour), nil
	}
	t, err := parseUpdateTime(lastUpdate)
	if err != nil {
		return time.Time{}, err
	}
	age := scanTime.Sub(t)
	if age < 0 {
		age = 0
	}
	return scanTime.Add(IntervalForAge(age)), nil
}

func parseUpdateTime(s string) (time.Time, error) {
	layouts := []string{
		"2006-01-02",
		"2006-01-02 15:04:05",
		time.RFC3339,
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported update time format %q", s)
}
