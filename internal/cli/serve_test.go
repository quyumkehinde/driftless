package cli

import (
	"testing"
	"time"
)

func TestNextAutoVerify(t *testing.T) {
	loc := time.FixedZone("test", 3600)
	tests := []struct {
		name string
		now  time.Time
		at   string
		want time.Time
	}{
		{
			"later today",
			time.Date(2026, 8, 18, 1, 30, 0, 0, loc), "03:00",
			time.Date(2026, 8, 18, 3, 0, 0, 0, loc),
		},
		{
			"already passed rolls to tomorrow",
			time.Date(2026, 8, 18, 3, 0, 1, 0, loc), "03:00",
			time.Date(2026, 8, 19, 3, 0, 0, 0, loc),
		},
		{
			"exactly now rolls to tomorrow",
			time.Date(2026, 8, 18, 3, 0, 0, 0, loc), "03:00",
			time.Date(2026, 8, 19, 3, 0, 0, 0, loc),
		},
		{
			"month boundary",
			time.Date(2026, 8, 31, 23, 59, 0, 0, loc), "00:30",
			time.Date(2026, 9, 1, 0, 30, 0, 0, loc),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := nextAutoVerify(tt.now, tt.at); !got.Equal(tt.want) {
				t.Errorf("nextAutoVerify(%v, %s) = %v, want %v", tt.now, tt.at, got, tt.want)
			}
		})
	}
}
