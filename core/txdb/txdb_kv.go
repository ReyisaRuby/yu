package txdb

import (
	"fmt"
	"time"

	. "github.com/yu-org/yu/common"
	. "github.com/yu-org/yu/core/types"
)

type PebbleGetErr struct {
	err error
}

func (p PebbleGetErr) Error() string {
	return p.err.Error()
}

const (
	txnLbl     = "txn"
	receiptLbl = "receipt"
)

func (t *txnkvdb) GetTxn(txnHash Hash) (txn *SignedTxn, err error) {
	var byt []byte
	//t.Lock()
	start := time.Now()
	byt, err = t.txnKV.Get(txnHash.Bytes())
	TxnDBInternalDuration.WithLabelValues(txnLbl, "GetTxn").Observe(float64(time.Since(start).Microseconds()))
	//t.Unlock()
	if err != nil {
		return nil, PebbleGetErr{err: err}
	}
	if byt == nil {
		return nil, nil
	}
	txn, err = DecodeSignedTxn(byt)
	if err != nil {
		return nil, err
	}
	return txn, nil
}

func (t *txnkvdb) GetTxns(txnHashList []Hash) ([]*SignedTxn, error) {
	results, err := t.getTxns(txnHashList)
	if err != nil {
		return nil, err
	}
	want := make([]*SignedTxn, 0, len(results))
	for _, result := range results {
		if result == nil {
			continue
		}
		txn, err := DecodeSignedTxn(result)
		if err != nil {
			return nil, err
		}
		want = append(want, txn)
	}
	return want, nil
}

func (t *txnkvdb) getTxns(txnHashList []Hash) ([][]byte, error) {
	results := make([][]byte, 0, len(txnHashList))
	//t.Lock()
	//defer t.Unlock()
	start := time.Now()
	defer func() {
		TxnDBInternalDuration.WithLabelValues(txnLbl, "getTxns").Observe(float64(time.Since(start).Microseconds()))
	}()
	for i := 0; i < len(txnHashList); i++ {
		byt, err := t.txnKV.Get(txnHashList[i].Bytes())
		if err != nil {
			return nil, err
		}
		results = append(results, byt)
	}
	return results, nil
}

func (t *txnkvdb) ExistTxn(txnHash Hash) bool {
	key := txnHash.Bytes()
	//t.Lock()
	//defer t.Unlock()
	start := time.Now()
	defer func() {
		TxnDBInternalDuration.WithLabelValues(txnLbl, "ExistTxn").Observe(float64(time.Since(start).Microseconds()))
	}()
	return t.txnKV.Exist(key)
}

func (t *txnkvdb) SetTxns(txns []*SignedTxn) (err error) {
	keys := make([][]byte, 0)
	values := make([][]byte, 0)
	for _, txn := range txns {
		txbyt, err := txn.Encode()
		if err != nil {
			return err
		}
		keys = append(keys, txn.TxnHash.Bytes())
		values = append(values, txbyt)
	}
	//t.Lock()
	//defer t.Unlock()
	start := time.Now()
	defer func() {
		TxnDBInternalDuration.WithLabelValues(txnLbl, "SetTxns").Observe(float64(time.Since(start).Microseconds()))
	}()
	kvtx, err := t.txnKV.NewKvTxn()
	if err != nil {
		return err
	}
	for i := 0; i < len(txns); i++ {
		err = kvtx.Set(keys[i], values[i])
		if err != nil {
			return err
		}
	}
	return kvtx.Commit()
}

func (r *receipttxnkvdb) GetReceipt(txHash Hash) (*Receipt, error) {
	return r.getReceipt(txHash)
}

func (r *receipttxnkvdb) getReceipt(txHash Hash) (*Receipt, error) {
	var byt []byte
	var err error
	//r.Lock()
	byt, err = r.receiptKV.Get(txHash.Bytes())
	start := time.Now()
	TxnDBInternalDuration.WithLabelValues(receiptLbl, "getReceipt").Observe(float64(time.Since(start).Microseconds()))
	//r.Unlock()
	if err != nil {
		return nil, PebbleGetErr{err: err}
	}
	if byt == nil || len(byt) < 1 {
		return nil, nil
	}
	receipt := new(Receipt)
	err = receipt.Decode(byt)
	if err != nil {
		return nil, err
	}
	return receipt, nil
}

func (r *receipttxnkvdb) GetReceipts(txHashList []Hash) ([]*Receipt, error) {
	if len(txHashList) > r.limit {
		return nil, fmt.Errorf("exceed GetReceipts limit %v", r.limit)
	}
	got, err := r.getReceipts(txHashList)
	if err != nil {
		return nil, err
	}
	results := make([]*Receipt, 0, len(txHashList))
	for _, byt := range got {
		if byt == nil || len(byt) < 1 {
			return nil, nil
		}
		receipt := new(Receipt)
		err = receipt.Decode(byt)
		if err != nil {
			receipt = nil
		}
		results = append(results, receipt)
	}
	return results, nil
}

func (r *receipttxnkvdb) getReceipts(txHashList []Hash) ([][]byte, error) {
	results := make([][]byte, 0, len(txHashList))
	//r.Lock()
	//defer r.Unlock()
	start := time.Now()
	defer func() {
		TxnDBInternalDuration.WithLabelValues(receiptLbl, "getReceipts").Observe(float64(time.Since(start).Microseconds()))
	}()
	for i := 0; i < len(txHashList); i++ {
		byt, err := r.receiptKV.Get(txHashList[i].Bytes())
		if err != nil {
			return nil, err
		}
		results = append(results, byt)
	}
	return results, nil
}

func (r *receipttxnkvdb) SetReceipt(txHash Hash, receipt *Receipt) error {
	byt, err := receipt.Encode()
	if err != nil {
		return err
	}
	key := txHash.Bytes()
	//r.Lock()
	//defer r.Unlock()
	start := time.Now()
	defer func() {
		TxnDBInternalDuration.WithLabelValues(receiptLbl, "SetReceipt").Observe(float64(time.Since(start).Microseconds()))
	}()
	return r.receiptKV.Set(key, byt)
}

func (r *receipttxnkvdb) SetReceipts(receipts map[Hash]*Receipt) error {
	keys := make([][]byte, 0)
	values := make([][]byte, 0)
	for txHash, receipt := range receipts {
		byt, err := receipt.Encode()
		if err != nil {
			return err
		}
		keys = append(keys, txHash.Bytes())
		values = append(values, byt)
	}
	//r.Lock()
	//defer r.Unlock()
	start := time.Now()
	defer func() {
		TxnDBInternalDuration.WithLabelValues(receiptLbl, "SetReceipts").Observe(float64(time.Since(start).Microseconds()))
	}()
	for i := 0; i < len(keys); i++ {
		err := r.receiptKV.Set(keys[i], values[i])
		if err != nil {
			return err
		}
	}
	return nil
}
