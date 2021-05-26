package cache

type value struct {
	UnixNano int64
	Value    byte
}

type values []value
