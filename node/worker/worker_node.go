package worker

import (
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"net/http"
	"yu/config"
	. "yu/node"
	"yu/storage/kv"
)

var WorkerInfoKey = []byte("worker-node-info")

type Worker struct {
	info *WorkerInfo

	port   string
	metadb kv.KV
}

func NewWorker(cfg *config.WorkerConf) (*Worker, error) {
	metadb, err := kv.NewKV(&cfg.DB)
	if err != nil {
		return nil, err
	}
	data, err := metadb.Get(WorkerInfoKey)
	if err != nil {
		return nil, err
	}
	var info *WorkerInfo
	if data == nil {
		info = &WorkerInfo{
			Name:           cfg.Name,
			NodeKeeperPort: ":" + cfg.NodeKeeperPort,
			Port:           ":" + cfg.ServesPort,
		}
		infoByt, err := info.EncodeMasterInfo()
		if err != nil {
			return nil, err
		}
		err = metadb.Set(WorkerInfoKey, infoByt)
		if err != nil {
			return nil, err
		}
	} else {
		info, err = DecodeWorkerInfo(data)
		if err != nil {
			return nil, err
		}
	}

	return &Worker{
		info:   info,
		port:   ":" + cfg.ServesPort,
		metadb: metadb,
	}, nil

}

func (w *Worker) HandleHttp() {
	r := gin.Default()

	r.GET(HeartbeatToPath, func(c *gin.Context) {
		c.JSON(http.StatusOK, nil)
		logrus.Debugf("accept heartbeat from %s", c.ClientIP())
	})

	r.Run(w.port)
}