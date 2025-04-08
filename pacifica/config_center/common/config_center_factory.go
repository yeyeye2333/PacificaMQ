package common

type ConfigCenterFactory interface {
	NewConfigCenter(InternalOptions) (ConfigCenter, error)
}
