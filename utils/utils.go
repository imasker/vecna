package utils

const LockKeyPrefix = "vecna_lock_"

func GetLockName(name, spec string) string {
	return LockKeyPrefix + name + spec
}
