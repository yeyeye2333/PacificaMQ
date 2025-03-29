package pacifica

import (
	. "github.com/yeyeye2333/PacificaMQ/pacifica/common"
)

type Snapshoter interface {
	Write(Index)
	Read() []byte
	Install([]byte)
}
