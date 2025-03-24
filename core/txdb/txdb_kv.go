package txdb

import (
	"fmt"

	. "github.com/yu-org/yu/common"
	. "github.com/yu-org/yu/core/types"
)

type PebbleGetErr struct {
	err error
}

func (p PebbleGetErr) Error() string {
	return p.err.Error()
}

func (t *txnkvdb) GetTxn(txnHash Hash) (txn *SignedTxn, err error) {
	var byt []byte
	t.Lock()
	byt, err = t.txnKV.Get(txnHash.Bytes())
	t.Unlock()
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
	t.Lock()
	defer t.Unlock()
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
	t.Lock()
	defer t.Unlock()
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
	t.Lock()
	defer t.Unlock()
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
	for i := 0; i < maxRetries; i++ {
		r.Lock()
		byt, err = r.receiptKV.Get(txHash.Bytes())
		r.Unlock()
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
	return nil, err
}

func (r *receipttxnkvdb) GetReceipts(txHashList []Hash) ([]*Receipt, error) {
	if len(txHashList) > 10 {
		return nil, fmt.Errorf("getReceipts size too big")
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
	r.Lock()
	defer r.Unlock()
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
	r.Lock()
	defer r.Unlock()
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
	r.Lock()
	defer r.Unlock()
	for i := 0; i < len(keys); i++ {
		err := r.receiptKV.Set(keys[i], values[i])
		if err != nil {
			return err
		}
	}
	return nil
}
