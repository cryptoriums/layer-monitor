package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	MetricsNamespace      = "layerc"
	DefaultRequestTimeout = 10 * time.Second

	// LoyaPerTRB is the number of loya (smallest unit) per TRB.
	// 1 TRB = 1,000,000 loya (6 decimals).
	LoyaPerTRB = 1_000_000
)

// ErrorsTotal is a unified error counter for all components.
// Use IncError() to increment with reason and component labels.
var ErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: MetricsNamespace,
	Name:      "errors_total",
	Help:      "Total errors across all components",
}, []string{"reason", "component"})

// IncError increments the unified error counter.
func IncError(reason, component string) {
	ErrorsTotal.WithLabelValues(reason, component).Inc()
}
