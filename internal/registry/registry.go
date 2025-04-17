package registry

import (
	"github.com/yeyeye2333/PacificaMQ/internal/extension"
	"github.com/yeyeye2333/PacificaMQ/internal/registry/common"
)

func NewRegistry(opts *common.Options) (common.Registry, error) {
	return extension.GetRegistry(opts.Name, opts.Internal)
}
