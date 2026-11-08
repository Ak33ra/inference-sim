package semiclosed

import (
	"fmt"
	"math"
	"math/rand"
)

type ArrivalConfig struct {
	Mode      string  `json:"mode"`
	IntervalS float64 `json:"interval_s,omitempty"`
	RatePerS  float64 `json:"rate_per_s,omitempty"`
	Seed      int64   `json:"seed"`
}

func secondsUs(value float64) (int64, error) {
	us := math.Round(value * 1e6)
	if value < 0 || math.IsNaN(us) || math.IsInf(us, 0) || us >= float64(math.MaxInt64) {
		return 0, fmt.Errorf("time must be finite, nonnegative, and fit the microsecond clock")
	}
	return int64(us), nil
}

func (a ArrivalConfig) Validate() error {
	if _, err := secondsUs(a.IntervalS); err != nil {
		return fmt.Errorf("arrival interval: %w", err)
	}
	switch a.Mode {
	case "", "trace":
		if a.IntervalS != 0 || a.RatePerS != 0 {
			return fmt.Errorf("trace arrivals do not accept interval or rate")
		}
	case "fixed":
		if a.RatePerS != 0 {
			return fmt.Errorf("fixed arrivals accept an interval, not a rate")
		}
	case "poisson":
		if a.IntervalS != 0 || math.IsNaN(a.RatePerS) || math.IsInf(a.RatePerS, 0) || a.RatePerS <= 0 {
			return fmt.Errorf("poisson arrivals require a finite positive rate and no interval")
		}
	default:
		return fmt.Errorf("unknown arrival mode %q; use trace, fixed, or poisson", a.Mode)
	}
	return nil
}

func (c *Corpus) arrivals(a ArrivalConfig) ([]int64, error) {
	if err := a.Validate(); err != nil {
		return nil, err
	}
	times := make([]int64, len(c.families))
	rng := rand.New(rand.NewSource(a.Seed))
	elapsed := float64(0)
	for i, f := range c.families {
		var err error
		switch a.Mode {
		case "", "trace":
			times[i] = (f.originMs - c.originMs) * 1000
			if c.arrivalUs != nil {
				times[i] = c.arrivalUs[i]
			}
		case "fixed":
			times[i], err = secondsUs(float64(i) * a.IntervalS)
		case "poisson":
			// Anchor the finite experiment at zero; subsequent gaps are exponential.
			if i > 0 {
				elapsed += rng.ExpFloat64() / a.RatePerS
			}
			times[i], err = secondsUs(elapsed)
		}
		if err != nil {
			return nil, fmt.Errorf("family %s arrival: %w", f.id, err)
		}
	}
	return times, nil
}
