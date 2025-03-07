package defaults

import (
	dft "github.com/creasty/defaults"
)

// `ptr` should be a struct pointer
func Set(ptr interface{}) error {
	return dft.Set(ptr)
}

func MustSet(ptr interface{}) {
	dft.MustSet(ptr)
}
