package common

type ClusterConfig struct {
	Version   uint64
	Leader    string
	Followers map[string]struct{}
}

func (cc *ClusterConfig) Init() {
	cc.Followers = make(map[string]struct{})
}
