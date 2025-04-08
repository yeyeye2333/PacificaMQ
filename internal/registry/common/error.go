package common

import "errors"

var (
	ErrNotSupport   = errors.New("not support")
	ErrLeaderExists = errors.New("leader exists")
)
