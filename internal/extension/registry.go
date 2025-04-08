package extension

import (
	"fmt"

	registry "github.com/yeyeye2333/PacificaMQ/internal/registry/common"
)

type RegistryFactory = func(registry.InternalOptions) (registry.Registry, error)

var registries = make(map[string]RegistryFactory)

func SetRegistry(name string, rf RegistryFactory) {
	registries[name] = rf
}

func GetRegistry(name string, options registry.InternalOptions) (registry.Registry, error) {
	if rf, ok := registries[name]; ok {
		return rf(options)
	}
	return nil, fmt.Errorf("registry %s not found", name)
}
