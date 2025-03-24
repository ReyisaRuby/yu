package txdb

import "github.com/prometheus/client_golang/prometheus"

const (
	TypeLbl = "type"
	OpLabel = "op"
)

var (
	TxnDBInternalDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "reddio",
			Subsystem: "txndb_internal",
			Name:      "duration_microseconds",
			Help:      "txn execute duration distribution.",
			Buckets:   prometheus.ExponentialBuckets(10, 2, 20), // 10us ~ 5s
		},
		[]string{TypeLbl, OpLabel},
	)
)

func init() {
	prometheus.MustRegister(TxnDBInternalDuration)
}
