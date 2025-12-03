package WritePath

import (
	"time"
)

type RateLimiting struct {
	Limit     int
	Available int
	Rate      int64
	Stored    int64
}

func CreateRateLimiting(limit int, rate int64) *RateLimiting {
	rm := RateLimiting{Limit: limit, Rate: rate}
	rm.Available = limit
	rm.Stored = Now()
	return &rm
}

func Now() int64 {
	return time.Now().Unix()
}

func (rm *RateLimiting) IsPast(moment int64) bool {
	return rm.Stored > moment-rm.Rate
}

func (rm *RateLimiting) IsAllowed() bool {
	now := Now()
	if !rm.IsPast(now) {
		if rm.Available > 0 {
			rm.Available -= 1
			return true
		} else {
			return false
		}
	} else {
		rm.Stored = now
		rm.Available = rm.Limit - 1
		return true
	}
}
