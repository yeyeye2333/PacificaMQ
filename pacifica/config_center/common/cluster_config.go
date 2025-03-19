package common

type ClusterConfig struct {
	Version   int
	Leader    string
	Followers map[string]struct{}
}
