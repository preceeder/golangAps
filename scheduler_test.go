package golangAps

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"
)

func TestNewScheduler(t *testing.T) {
	// 注册任务函数
	RegisterJobsFunc(FuncInfo{
		Func:        R02Test,
		Name:        "test",
		Description: "初次测试",
	})

	// 创建scheduler
	sp := NewScheduler(DefaultDbPath)
	//// 创建存储器， 不创建也可以， 在添加任务的时候会自动创建
	//err := sp.SetStore("test-001")
	//if err != nil {
	//	fmt.Println(err.Error())
	//	return
	//}

	sp.Start()

	job, err := sp.AddJob(Job{
		Name: "test",
		Id:   "test01",
		Trigger: &IntervalTrigger{
			Interval: 5,
		},
		FuncName:  "test",
		StoreName: DefaultStoreName,
		Replace:   true,
	})
	if err != nil {
		fmt.Println("添加任务错误：", err.Error())
		return
	}
	fmt.Println(job.String())

	job3, err := sp.AddJob(Job{
		Name: "test",
		Id:   "test05",
		Trigger: &IntervalTrigger{
			Interval:     2,
			TimeZoneName: "UTC+8",
		},
		Args: map[string]any{
			"name": "万三",
		},
		FuncName:  "test",
		StoreName: "test-api",
		Replace:   true,
	})
	if err != nil {
		fmt.Println("添加任务错误：", err.Error())
		return
	}
	fmt.Println(job3.String())
	//
	//job2, err := sp.AddJob(Job{
	//	Name: "test",
	//	Id:   "test01",
	//	Trigger: &IntervalTrigger{
	//		Interval: 5,
	//	},
	//	FuncName:  "test",
	//	StoreName: DefaultStoreName,
	//	Replace:   true,
	//})
	//if err != nil {
	//	fmt.Println("添加任务错误：", err.Error())
	//	return
	//}
	//
	//fmt.Println(job2.String())
	signalLister(func() {
		sp.Stop()
	})
}

func R02Test(j Job) any {
	fmt.Println("执行测试任务：", time.Now().Format(time.DateTime), j)
	return nil
}

func signalLister(f func()) {
	c := make(chan os.Signal)
	//监听指定信号 ctrl+c kill
	signal.Notify(c, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM,
		syscall.SIGQUIT)

	for s := range c {
		switch s {
		case syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT:
			f()
			slog.Info("SignalHandler Over", "sign", s.String())
			return
		default:
			slog.Info("other signal", "sign", s.String())
		}
	}
	return
}
