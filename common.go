package golangAps

import (
	"context"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
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

// SignalListener 监听系统信号并执行清理函数
// 支持 SIGINT (Ctrl+C) 和 SIGTERM (kill 命令)
// cleanup 函数会在收到信号时被调用，用于优雅关闭应用
// 如果 cleanup 函数执行时间超过 30 秒，将强制退出
func SignalListener(cleanup func()) {
	// 创建信号通道
	sigChan := make(chan os.Signal, 1)

	// 注册要监听的信号
	// SIGINT: 通常由 Ctrl+C 触发，所有平台都支持
	// SIGTERM: 通常由 kill 命令触发，Unix/Linux 支持
	signals := []os.Signal{os.Interrupt}
	// 检查 SIGTERM 是否可用（Windows 上可能不可用）
	if syscall.SIGTERM != 0 {
		signals = append(signals, syscall.SIGTERM)
	}
	signal.Notify(sigChan, signals...)

	// 在单独的 goroutine 中等待信号
	go func() {
		sig := <-sigChan
		DefaultLog.Info(context.Background(), "收到系统信号，开始优雅关闭", "signal", sig.String())

		// 执行清理函数，设置超时保护
		if cleanup != nil {
			done := make(chan struct{})
			go func() {
				defer CatchException(func(err any) {
					DefaultLog.Error(context.Background(), "清理函数执行出错", "error", err)
				})
				cleanup()
				close(done)
			}()

			// 等待清理完成，最多等待 30 秒
			select {
			case <-done:
				DefaultLog.Info(context.Background(), "清理函数执行完成")
			case <-time.After(30 * time.Second):
				DefaultLog.Warn(context.Background(), "清理函数执行超时，强制退出")
			}
		}

		DefaultLog.Info(context.Background(), "应用已优雅关闭")
		os.Exit(0)
	}()
}
