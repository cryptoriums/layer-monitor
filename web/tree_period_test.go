package web

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The page renders the StatsPeriodDays button as active and the validator tree is
// fetched separately. When the two disagreed, a fresh load showed "30 Days"
// selected while the validator cards below it carried 7-day numbers.
func TestDisplayPeriodMatchesActivePeriodButton(t *testing.T) {
	s := &Server{cfg: Config{LookbackPeriodDays: 7, StatsPeriodDays: 30}}

	// handleRoot marks the StatsPeriodDays button active; the tree must agree.
	if got, want := s.displayPeriodDays(), s.cfg.StatsPeriodDays; got != want {
		t.Fatalf("displayPeriodDays() = %d, want %d (the period the page shows as active)", got, want)
	}
}

// LookbackPeriodDays is wired to the backfill window, which says nothing about what
// should be displayed. It must not leak into the default display period.
func TestDisplayPeriodIgnoresBackfillLookback(t *testing.T) {
	for _, lookback := range []int{1, 7, 14, 90} {
		s := &Server{cfg: Config{LookbackPeriodDays: lookback, StatsPeriodDays: 30}}
		if got := s.displayPeriodDays(); got != 30 {
			t.Fatalf("lookback=%d: displayPeriodDays() = %d, want 30", lookback, got)
		}
	}
}

func TestDisplayPeriodFallsBackWhenUnset(t *testing.T) {
	s := &Server{cfg: Config{LookbackPeriodDays: 7}}
	if got := s.displayPeriodDays(); got != DefaultStatsPeriodDays {
		t.Fatalf("displayPeriodDays() = %d, want %d", got, DefaultStatsPeriodDays)
	}
}

// Guards the other half of the pairing: the template must mark active whichever
// period handleRoot computed, so a selection and the tree cannot drift apart.
func TestTemplateMarksActiveButtonFromPeriodDays(t *testing.T) {
	tmpl, err := os.ReadFile(filepath.Join(assetsDir, "template.html"))
	if err != nil {
		t.Fatalf("read template: %v", err)
	}
	for _, p := range []string{"1", "7", "30"} {
		want := `<a href="?period=` + p + `#network" class="period-btn{{if eq .PeriodDays ` + p + `}} period-btn-active{{end}}">`
		if !strings.Contains(string(tmpl), want) {
			t.Fatalf("template is missing the PeriodDays-driven active marker for period %s", p)
		}
	}
}
