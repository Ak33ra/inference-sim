package cmd

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResearchSchedulerConfigValidation(t *testing.T) {
	for _, body := range []string{`{"quanta_ms":[]}`, `{"quanta_ms":[0]}`, `{"quanta_ms":[-1]}`, `{"starvation_ratio":0}`, `{"unknown":1}`, `{} {}`, `null`} {
		t.Run(body, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "scheduler.json")
			if err := os.WriteFile(p, []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := readResearchSchedulerConfig(p); err == nil {
				t.Fatal("accepted invalid config")
			}
		})
	}
	p := filepath.Join(t.TempDir(), "scheduler.json")
	os.WriteFile(p, []byte(`{"quanta_ms":[100,200],"starvation_ratio":10}`), 0600)
	c, err := readResearchSchedulerConfig(p)
	if err != nil || len(c.QuantaMS) != 2 {
		t.Fatalf("valid config: %+v %v", c, err)
	}
}
