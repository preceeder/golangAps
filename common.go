package golangAps

import (
	"math/rand"
	"sync"
)

var letters = []rune("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
var lettersInt = []rune("0123456789")

func RandStr(str_len int) string {
	rand_bytes := make([]rune, str_len)
	for i := range rand_bytes {
		rand_bytes[i] = letters[rand.Intn(len(letters))]
	}
	return string(rand_bytes)
}

type Instance[T int] struct {
	Instances sync.Map
}

func (i *Instance[T]) Add(key string, num T) {
	if v, ok := i.Instances.Load(key); ok {
		i.Instances.Store(key, v.(T)+num)
	} else {
		i.Instances.Store(key, num)
	}
}

func (i *Instance[T]) Sub(key string, num T) {
	if v, ok := i.Instances.Load(key); ok {
		i.Instances.Store(key, v.(T)-num)
	} else {
		i.Instances.Store(key, -num)
	}
}

func (i *Instance[T]) Get(key string) (res T) {
	if v, ok := i.Instances.Load(key); ok {
		res = v.(T)
	} else {
		return
	}
	return
}
