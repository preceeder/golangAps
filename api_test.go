package golangAps

import (
	"fmt"
	"testing"
	"time"
)

func R03Test(j Job) any {
	fmt.Println("执行测试任务：", time.Now().Format(time.DateTime), j)
	return nil
}

func TestRunAdmin(t *testing.T) {
	// 注册任务函数
	RegisterJobsFunc(FuncInfo{
		Func:        R03Test,
		Name:        "test03",
		Description: "初次测试",
	})
	RegisterJobsFunc(FuncInfo{
		Func:        R03Test,
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
	//ec := RunAdmin(sp, ":8080")
	//signalLister(func() {
	//	err := ec.Shutdown(context.Background())
	//	if err != nil {
	//		fmt.Println("关闭echo 异常", err.Error())
	//	}
	//	sp.Stop()
	//})
}
