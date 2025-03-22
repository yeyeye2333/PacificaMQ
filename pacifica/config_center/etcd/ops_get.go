package etcd

import (
	"github.com/yeyeye2333/PacificaMQ/pkg/conv"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// 操作执行或解析都得将version放在最后
func (cc *etcdConfigCenter) replaceLeaderOP(new_leader string, new_version uint64, isInit bool) []clientv3.Op {
	if isInit {
		return []clientv3.Op{
			clientv3.OpPut(cc.leaderPrefix, new_leader),
			clientv3.OpPut(cc.versionPrefix, string(conv.Uint64ToBytes(new_version))),
		}
	}
	return []clientv3.Op{
		clientv3.OpPut(cc.leaderPrefix, new_leader),
		clientv3.OpDelete(cc.followersPrefix + Sep + new_leader),
		clientv3.OpPut(cc.versionPrefix, string(conv.Uint64ToBytes(new_version))),
	}

}

func (cc *etcdConfigCenter) addFollowerOP(follower string, new_version uint64) []clientv3.Op {
	return []clientv3.Op{
		clientv3.OpPut(cc.followersPrefix+Sep+follower, ""),
		clientv3.OpPut(cc.versionPrefix, string(conv.Uint64ToBytes(new_version))),
	}
}

func (cc *etcdConfigCenter) removeFollowerOP(follower string, new_version uint64) []clientv3.Op {
	return []clientv3.Op{
		clientv3.OpDelete(cc.followersPrefix + Sep + follower),
		clientv3.OpPut(cc.versionPrefix, string(conv.Uint64ToBytes(new_version))),
	}
}
