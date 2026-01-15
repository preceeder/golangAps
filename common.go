package golangAps

import (
	"math/rand"
	"sync"
	"time"
)

var (
	letters    = []rune("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	lettersInt = []rune("0123456789")
	randMu     sync.Mutex
	randSrc    = rand.New(rand.NewSource(time.Now().UnixNano()))
)

func RandStr(str_len int) string {
	randMu.Lock()
	defer randMu.Unlock()
	rand_bytes := make([]rune, str_len)
	for i := range rand_bytes {
		rand_bytes[i] = letters[randSrc.Intn(len(letters))]
	}
	return string(rand_bytes)
}

type Instance[T int] struct {
	Instances sync.Map
}

func (i *Instance[T]) Add(key string, num T) {
	// 使用 LoadOrStore 确保原子性
	for {
		if v, ok := i.Instances.Load(key); ok {
			oldVal := v.(T)
			newVal := oldVal + num
			if i.Instances.CompareAndSwap(key, oldVal, newVal) {
				return
			}
			// 如果 CompareAndSwap 失败，重试
		} else {
			// 如果不存在，尝试存储初始值
			if _, loaded := i.Instances.LoadOrStore(key, num); !loaded {
				return
			}
			// 如果 LoadOrStore 返回 loaded=true，说明其他 goroutine 已经存储了值，重试
		}
	}
}

func (i *Instance[T]) Sub(key string, num T) {
	// 使用 CompareAndSwap 确保原子性和防止负数
	for {
		if v, ok := i.Instances.Load(key); ok {
			oldVal := v.(T)
			newVal := oldVal - num
			// 防止负数
			if newVal < 0 {
				newVal = 0
			}
			if i.Instances.CompareAndSwap(key, oldVal, newVal) {
				return
			}
			// 如果 CompareAndSwap 失败，重试
		} else {
			// 如果不存在，不执行减法操作（避免负数）
			return
		}
	}
}

func (i *Instance[T]) Get(key string) (res T) {
	if v, ok := i.Instances.Load(key); ok {
		res = v.(T)
		return res
	}
	return 0
}
