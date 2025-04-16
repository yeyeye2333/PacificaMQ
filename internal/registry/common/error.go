package common

import "errors"

var (
	ErrNotSupport   = errors.New("not support")
	ErrBrokerBehind = errors.New("broker behind")
	ErrLeaderExists = errors.New("leader exists")
)
