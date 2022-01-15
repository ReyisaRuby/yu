package main

import (
	"github.com/sirupsen/logrus"
	"github.com/yu-org/yu/apps/asset"
	"github.com/yu-org/yu/apps/poa"
	"github.com/yu-org/yu/common"
	"github.com/yu-org/yu/core/keypair"
	"github.com/yu-org/yu/core/startup"
	"os"
	"strconv"
)

type pair struct {
	pubkey  keypair.PubKey
	privkey keypair.PrivKey
}

func main() {
	pub0, priv0 := keypair.GenSrKey([]byte("node1"))
	logrus.Info("node1 address is ", pub0.Address().String())

	pub1, priv1 := keypair.GenSrKey([]byte("node2"))
	logrus.Info("node2 address is ", pub1.Address().String())

	pub2, priv2 := keypair.GenSrKey([]byte("node3"))
	logrus.Info("node3 address is ", pub2.Address().String())

	pairArray := []pair{
		{
			pubkey:  pub0,
			privkey: priv0,
		},
		{
			pubkey:  pub1,
			privkey: priv1,
		},
		{
			pubkey:  pub2,
			privkey: priv2,
		},
	}

	idxStr := os.Args[1]
	idx, err := strconv.Atoi(idxStr)
	if err != nil {
		panic(err)
	}

	myPubkey := pairArray[idx].pubkey
	myPrivkey := pairArray[idx].privkey

	validatorsMap := map[common.Address]string{
		pub0.Address(): "12D3KooWHHzSeKaY8xuZVzkLbKFfvNgPPeKhFBGrMbNzbm5akpqu",
		pub1.Address(): "12D3KooWSKPs95miv8wzj3fa5HkJ1tH7oEGumsEiD92n2MYwRtQG",
		pub2.Address(): "12D3KooWRuwP7nXaRhZrmoFJvPPGat2xPafVmGpQpZs5zKMtwqPH",
	}
	logrus.Info("My Address is ", pairArray[idx].pubkey.Address().String())
	startup.StartUp(poa.NewPoa(myPubkey, myPrivkey, validatorsMap), asset.NewAsset("YuCoin"))
}