package txdb

import (
	"sync"
	"time"

	. "github.com/yu-org/yu/common"
	"github.com/yu-org/yu/config"
	. "github.com/yu-org/yu/core/types"
	"github.com/yu-org/yu/infra/storage/kv"
	"github.com/yu-org/yu/metrics"
)

const (
	Txns          = "txns"
	Results       = "results"
	maxRetries    = 3
	retryInterval = 50 * time.Millisecond
)

type TxDB struct {
	nodeType int

	txnKV     *txnkvdb
	receiptKV *receipttxnkvdb
}

type txnkvdb struct {
	*sync.Mutex
	txnKV kv.KV
}

const (
	kvSourceType   = "kv"
	txnType        = "txn"
	receiptType    = "receipt"
	successStatus  = "success"
	errStatus      = "err"
	pbErrStatus    = "pbErr"
	notFoundStatus = "notFound"
)

func getStatusValue(err error) string {
	if err == nil {
		return successStatus
	}
	return errStatus
}

func NewTxDB(nodeTyp int, kvdb kv.Kvdb, txnConf config.TxnConf) (ItxDB, error) {
	mutex := &sync.Mutex{}
	txdb := &TxDB{
		nodeType:  nodeTyp,
		txnKV:     &txnkvdb{txnKV: kvdb.New(Txns), Mutex: mutex},
		receiptKV: &receipttxnkvdb{receiptKV: kvdb.New(Results), Mutex: mutex},
	}
	return txdb, nil
}

func (bb *TxDB) GetTxn(txnHash Hash) (stxn *SignedTxn, err error) {
	if bb.nodeType == LightNode {
		return nil, nil
	}
	start := time.Now()
	defer func() {
		metrics.TxnDBDuration.WithLabelValues(txnType, "getTxn").Observe(float64(time.Since(start).Microseconds()))
	}()
	txn, err := bb.txnKV.GetTxn(txnHash)
	if err != nil {
		metrics.TxnDBCounter.WithLabelValues(txnType, kvSourceType, "getTxn", errStatus).Inc()
		return nil, err
	}
	metrics.TxnDBCounter.WithLabelValues(txnType, kvSourceType, "getTxn", successStatus).Inc()
	return txn, nil
}

func (bb *TxDB) GetTxns(txnHashes []Hash) (stxns []*SignedTxn, err error) {
	if bb.nodeType == LightNode {
		return nil, nil
	}
	start := time.Now()
	defer func() {
		metrics.TxnDBDuration.WithLabelValues(txnType, "getTxns").Observe(float64(time.Since(start).Microseconds()))
	}()
	want, err := bb.txnKV.GetTxns(txnHashes)
	if err != nil {
		metrics.TxnDBCounter.WithLabelValues(txnType, kvSourceType, "getTxns", errStatus).Inc()
		return nil, err
	}
	metrics.TxnDBCounter.WithLabelValues(txnType, kvSourceType, "getTxns", successStatus).Inc()
	return want, nil
}

func (bb *TxDB) ExistTxn(txnHash Hash) bool {
	if bb.nodeType == LightNode {
		return false
	}
	start := time.Now()
	defer func() {
		metrics.TxnDBDuration.WithLabelValues(txnType, "exist").Observe(float64(time.Since(start).Microseconds()))
	}()
	find := bb.txnKV.ExistTxn(txnHash)
	metrics.TxnDBCounter.WithLabelValues(txnType, kvSourceType, "exist", successStatus).Inc()
	return find
}

func (bb *TxDB) SetTxns(txns []*SignedTxn) (err error) {
	if bb.nodeType == LightNode {
		return nil
	}
	start := time.Now()
	defer func() {
		metrics.TxnDBDuration.WithLabelValues(txnType, "setTxns").Observe(float64(time.Since(start).Microseconds()))
		metrics.TxnDBCounter.WithLabelValues(txnType, kvSourceType, "setTxns", getStatusValue(err)).Inc()
	}()
	return bb.txnKV.SetTxns(txns)
}

func (bb *TxDB) SetReceipts(receipts map[Hash]*Receipt) (err error) {
	start := time.Now()
	defer func() {
		metrics.TxnDBDuration.WithLabelValues(receiptType, "setReceipts").Observe(float64(time.Since(start).Microseconds()))
		metrics.TxnDBCounter.WithLabelValues(receiptType, kvSourceType, "setReceipts", getStatusValue(err)).Inc()
	}()
	return bb.receiptKV.SetReceipts(receipts)
}

func (bb *TxDB) SetReceipt(txHash Hash, receipt *Receipt) (err error) {
	start := time.Now()
	defer func() {
		metrics.TxnDBDuration.WithLabelValues(receiptType, "setReceipt").Observe(float64(time.Since(start).Microseconds()))
		metrics.TxnDBCounter.WithLabelValues(receiptType, kvSourceType, "setReceipt", getStatusValue(err)).Inc()
	}()
	return bb.receiptKV.SetReceipt(txHash, receipt)
}

func (bb *TxDB) GetReceipt(txHash Hash) (rec *Receipt, err error) {
	start := time.Now()
	defer func() {
		metrics.TxnDBDuration.WithLabelValues(receiptType, "getReceipt").Observe(float64(time.Since(start).Microseconds()))
	}()
	r, err := bb.receiptKV.GetReceipt(txHash)
	if err != nil {
		if pbErr, ok := err.(PebbleGetErr); ok {
			metrics.TxnDBCounter.WithLabelValues(receiptType, kvSourceType, "getReceipt", pbErrStatus).Inc()
			return nil, pbErr.err
		}
		metrics.TxnDBCounter.WithLabelValues(receiptType, kvSourceType, "getReceipt", errStatus).Inc()
		return nil, err
	}
	if r != nil {
		metrics.TxnDBCounter.WithLabelValues(receiptType, kvSourceType, "getReceipt", successStatus).Inc()
	} else {
		metrics.TxnDBCounter.WithLabelValues(receiptType, kvSourceType, "getReceipt", notFoundStatus).Inc()
	}
	return r, nil
}

func (bb *TxDB) GetReceipts(txHashList []Hash) (rec []*Receipt, err error) {
	start := time.Now()
	defer func() {
		metrics.TxnDBDuration.WithLabelValues(receiptType, "getReceipts").Observe(float64(time.Since(start).Microseconds()))
	}()
	r, err := bb.receiptKV.GetReceipts(txHashList)
	if err != nil {
		if pbErr, ok := err.(PebbleGetErr); ok {
			metrics.TxnDBCounter.WithLabelValues(receiptType, kvSourceType, "getReceipts", pbErrStatus).Inc()
			return nil, pbErr.err
		}
		metrics.TxnDBCounter.WithLabelValues(receiptType, kvSourceType, "getReceipts", errStatus).Inc()
		return nil, err
	}
	metrics.TxnDBCounter.WithLabelValues(receiptType, kvSourceType, "getReceipts", successStatus).Inc()
	return r, nil
}

type receipttxnkvdb struct {
	*sync.Mutex
	receiptKV kv.KV
}
