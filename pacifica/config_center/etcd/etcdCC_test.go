package etcd

import (
	"context"
	"testing"

	"github.com/yeyeye2333/PacificaMQ/internal/logger"
	"github.com/yeyeye2333/PacificaMQ/pacifica/config_center"
	"github.com/yeyeye2333/PacificaMQ/pacifica/config_center/common"
	"github.com/yeyeye2333/PacificaMQ/pkg/conv"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func newEtcdConfigCenter() (common.ConfigCenter, error) {
	logger.SetLevel("debug")
	return config_center.NewConfigCenter(common.WithName("etcd"))
}
func Test_ConfigCenter(t *testing.T) {
	cc, err := newEtcdConfigCenter()
	if err != nil {
		t.Error(err)
	}
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints: []string{"127.0.0.1:2379"},
	})
	if err != nil {
		t.Error(err)
	}

	cc.Start()
	defer cc.Close()

	config, _ := cc.GetConfig()
	t.Log(config)
	if config.Version == 0 {
		err = cc.ReplaceLeader("testNode1", config.Version+1)
		if err != nil {
			t.Error(err)
		}
		config, _ := cc.GetConfig()
		t.Log(config)

		config, _ = cc.GetConfig()
		if config.Leader != "testNode1" || config.Version != 1 {
			t.Error("replace leader failed")
		}
		realGetLeader, err := etcdCli.Get(context.TODO(), "/Pacifica/Leader")
		if err != nil {
			t.Errorf("etcd Get Error: %s", err.Error())
		}
		if len(realGetLeader.Kvs) != 1 {
			t.Errorf("etcd Get Length Error: %d", len(realGetLeader.Kvs))
		} else if string(realGetLeader.Kvs[0].Value) != "testNode1" {
			t.Errorf("etcd Get testNode1 Conflict: %s", string(realGetLeader.Kvs[0].Value))
		}
		realGetVersion, _ := etcdCli.Get(context.TODO(), "/Pacifica/Version")
		if conv.BytesToInt32(realGetVersion.Kvs[0].Value) != 1 {
			t.Error("etcd Get Version Conflict")
		}
	} else {
		t.Error("etcd config center version error")
	}
}
