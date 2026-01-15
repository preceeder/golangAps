package golangAps

import (
	"sync"
	"testing"
	"time"
)

// 测试用的任务函数
var testJobExecCount sync.Map

func testJobFunc(j Job) any {
	count, _ := testJobExecCount.LoadOrStore(j.Id, int(0))
	testJobExecCount.Store(j.Id, count.(int)+1)
	return nil
}

func TestNewScheduler(t *testing.T) {

	scheduler := NewScheduler("")
	if scheduler == nil {
		t.Fatal("NewScheduler returned nil")
	}
	if scheduler.IsRunning() {
		t.Error("New scheduler should not be running")
	}
}

func TestScheduler_StartStop(t *testing.T) {
	scheduler := NewScheduler("")

	// 测试 Start
	scheduler.Start()
	if !scheduler.IsRunning() {
		t.Error("Scheduler should be running after Start()")
	}

	// 测试重复 Start
	scheduler.Start() // 应该不会报错

	// 测试 Stop
	scheduler.Stop()
	if scheduler.IsRunning() {
		t.Error("Scheduler should not be running after Stop()")
	}

	// 测试重复 Stop
	scheduler.Stop() // 应该不会报错
}

func TestScheduler_AddJob(t *testing.T) {

	RegisterJobsFunc(FuncInfo{
		Func:        testJobFunc,
		Name:        "testJobFunc",
		Description: "Test job function",
	})

	scheduler := NewScheduler("")
	scheduler.Start()
	defer scheduler.Stop()

	job := Job{
		Id:        "test-job-1",
		Name:      "Test Job",
		FuncName:  "testJobFunc",
		StoreName: "test-store",
		Replace:   true,
		Trigger: &IntervalTrigger{
			Interval: 10,
		},
	}

	// 测试添加任务
	result, err := scheduler.AddJob(job)
	if err != nil {
		t.Fatalf("AddJob failed: %v", err)
	}
	if result.Id != job.Id {
		t.Errorf("Expected job ID %s, got %s", job.Id, result.Id)
	}

	// 测试添加重复任务（不替换）
	job.Replace = false
	_, err = scheduler.AddJob(job)
	if err == nil {
		t.Error("AddJob should fail when job exists and Replace is false")
	}

	// 测试添加无效任务（缺少必要字段）
	invalidJob := Job{
		Id:   "",
		Name: "Invalid",
	}
	_, err = scheduler.AddJob(invalidJob)
	if err == nil {
		t.Error("AddJob should fail for invalid job")
	}

	// 测试添加未注册函数名的任务
	invalidJob2 := Job{
		Id:        "invalid-job",
		Name:      "Invalid",
		FuncName:  "NonExistentFunc",
		StoreName: "test-store",
		Trigger:   &IntervalTrigger{Interval: 10},
	}
	_, err = scheduler.AddJob(invalidJob2)
	if err == nil {
		t.Error("AddJob should fail for unregistered function")
	}
}

func TestScheduler_UpdateJob(t *testing.T) {

	RegisterJobsFunc(FuncInfo{
		Func:        testJobFunc,
		Name:        "testJobFunc",
		Description: "Test job function",
	})

	scheduler := NewScheduler("")
	scheduler.Start()
	defer scheduler.Stop()

	job := Job{
		Id:        "test-job-update",
		Name:      "Original Name",
		FuncName:  "testJobFunc",
		StoreName: "test-store",
		Replace:   true,
		Trigger: &IntervalTrigger{
			Interval: 10,
		},
	}

	// 添加任务
	_, err := scheduler.AddJob(job)
	if err != nil {
		t.Fatalf("AddJob failed: %v", err)
	}

	// 更新任务
	job.Name = "Updated Name"
	job.Timeout = 30
	result, err := scheduler.UpdateJob(job)
	if err != nil {
		t.Fatalf("UpdateJob failed: %v", err)
	}
	if result.Name != "Updated Name" {
		t.Errorf("Expected name 'Updated Name', got '%s'", result.Name)
	}
	if result.Timeout != 30 {
		t.Errorf("Expected timeout 30, got %d", result.Timeout)
	}

	// 测试更新不存在的任务
	job.Id = "non-existent"
	_, err = scheduler.UpdateJob(job)
	if err != nil {
		t.Error("UpdateJob should fail for non-existent job", err.Error())
	}
}

func TestScheduler_DeleteJob(t *testing.T) {

	RegisterJobsFunc(FuncInfo{
		Func:        testJobFunc,
		Name:        "testJobFunc",
		Description: "Test job function",
	})

	scheduler := NewScheduler("")
	scheduler.Start()
	defer scheduler.Stop()

	job := Job{
		Id:        "test-job-delete",
		Name:      "Test Job",
		FuncName:  "testJobFunc",
		StoreName: "test-store",
		Replace:   true,
		Trigger:   &IntervalTrigger{Interval: 10},
	}

	// 添加任务
	_, err := scheduler.AddJob(job)
	if err != nil {
		t.Fatalf("AddJob failed: %v", err)
	}

	// 删除任务
	err = scheduler.DeleteJob("test-store", "test-job-delete")
	if err != nil {
		t.Fatalf("DeleteJob failed: %v", err)
	}

	// 验证任务已删除
	_, err = scheduler.QueryJob("test-store", "test-job-delete")
	if err != nil {
		t.Error("Job should be deleted")
	}

	// 测试删除不存在的任务
	err = scheduler.DeleteJob("test-store", "non-existent")
	if err != nil {
		t.Error("DeleteJob should return error for non-existent job")
	}
}

func TestScheduler_PauseResumeJob(t *testing.T) {

	RegisterJobsFunc(FuncInfo{
		Func:        testJobFunc,
		Name:        "testJobFunc",
		Description: "Test job function",
	})

	scheduler := NewScheduler("")
	scheduler.Start()
	defer scheduler.Stop()

	job := Job{
		Id:        "test-job-pause",
		Name:      "Test Job",
		FuncName:  "testJobFunc",
		StoreName: "test-store",
		Replace:   true,
		Trigger:   &IntervalTrigger{Interval: 5},
	}

	// 添加任务
	_, err := scheduler.AddJob(job)
	if err != nil {
		t.Fatalf("AddJob failed: %v", err)
	}

	// 暂停任务
	pausedJob, err := scheduler.PauseJob("test-store", "test-job-pause")
	if err != nil {
		t.Fatalf("PauseJob failed: %v", err)
	}
	if pausedJob.Status != STATUS_PAUSED {
		t.Errorf("Expected status PAUSED, got %s", pausedJob.Status)
	}

	// 恢复任务
	resumedJob, err := scheduler.ResumeJob("test-store", "test-job-pause")
	if err != nil {
		t.Fatalf("ResumeJob failed: %v", err)
	}
	if resumedJob.Status != STATUS_RUNNING {
		t.Errorf("Expected status RUNNING, got %s", resumedJob.Status)
	}

	// 测试暂停不存在的任务
	_, err = scheduler.PauseJob("test-store", "non-existent")
	if err != nil {
		t.Error("PauseJob should fail for non-existent job")
	}
}

func TestScheduler_QueryJob(t *testing.T) {

	RegisterJobsFunc(FuncInfo{
		Func:        testJobFunc,
		Name:        "testJobFunc",
		Description: "Test job function",
	})

	scheduler := NewScheduler("")
	scheduler.Start()
	defer scheduler.Stop()

	job := Job{
		Id:        "test-job-query",
		Name:      "Test Job",
		FuncName:  "testJobFunc",
		StoreName: "test-store",
		Replace:   true,
		Trigger:   &IntervalTrigger{Interval: 10},
	}

	// 添加任务
	_, err := scheduler.AddJob(job)
	if err != nil {
		t.Fatalf("AddJob failed: %v", err)
	}

	// 查询任务
	queriedJob, err := scheduler.QueryJob("test-store", "test-job-query")
	if err != nil {
		t.Fatalf("QueryJob failed: %v", err)
	}
	if queriedJob.Id != job.Id {
		t.Errorf("Expected job ID %s, got %s", job.Id, queriedJob.Id)
	}

	// 查询不存在的任务
	_, err = scheduler.QueryJob("test-store", "non-existent")
	if err != nil {
		t.Error("QueryJob should fail for non-existent job")
	}
}

func TestScheduler_GetJobsByStoreName(t *testing.T) {

	RegisterJobsFunc(FuncInfo{
		Func:        testJobFunc,
		Name:        "testJobFunc",
		Description: "Test job function",
	})

	scheduler := NewScheduler("")
	scheduler.Start()
	defer scheduler.Stop()

	storeName := "test-store-multi"

	// 添加多个任务
	for i := 0; i < 5; i++ {
		job := Job{
			Id:        "test-job-" + string(rune('a'+i)),
			Name:      "Test Job",
			FuncName:  "testJobFunc",
			StoreName: storeName,
			Replace:   true,
			Trigger:   &IntervalTrigger{Interval: 10},
		}
		_, err := scheduler.AddJob(job)
		if err != nil {
			t.Fatalf("AddJob failed: %v", err)
		}
	}

	// 获取任务列表
	jobs, hasMore := scheduler.GetJobsByStoreName(storeName, 0, 10)
	if len(jobs) != 5 {
		t.Errorf("Expected 5 jobs, got %d", len(jobs))
	}
	if hasMore {
		t.Error("Should not have more jobs")
	}

	// 测试分页
	jobs, hasMore = scheduler.GetJobsByStoreName(storeName, 0, 2)
	if len(jobs) != 2 {
		t.Errorf("Expected 2 jobs, got %d", len(jobs))
	}
	if !hasMore {
		t.Error("Should have more jobs")
	}
}

func TestScheduler_SearchJobById(t *testing.T) {
	RegisterJobsFunc(FuncInfo{
		Func:        testJobFunc,
		Name:        "testJobFunc",
		Description: "Test job function",
	})

	scheduler := NewScheduler("")
	scheduler.Start()
	defer scheduler.Stop()

	storeName := "test-store-search"

	// 添加多个任务
	prefixes := []string{"task-", "task-", "job-", "job-"}
	for i, prefix := range prefixes {
		job := Job{
			Id:        prefix + string(rune('a'+i)),
			Name:      "Test Job",
			FuncName:  "testJobFunc",
			StoreName: storeName,
			Replace:   true,
			Trigger:   &IntervalTrigger{Interval: 10},
		}
		_, err := scheduler.AddJob(job)
		if err != nil {
			t.Fatalf("AddJob failed: %v", err)
		}
	}

	// 搜索任务
	jobs, nextId, err := scheduler.SearchJobById(storeName, "task-", "", 10)
	if err != nil {
		t.Fatalf("SearchJobById failed: %v", err)
	}
	if len(jobs) != 2 {
		t.Errorf("Expected 2 jobs with prefix 'task-', got %d", len(jobs))
	}
	if nextId == "" {
		t.Error("nextId should not be empty")
	}
}

func TestScheduler_ImmediatelyRunJob(t *testing.T) {

	testJobExecCount.Store("immediate-job", 0)

	RegisterJobsFunc(FuncInfo{
		Func:        testJobFunc,
		Name:        "testJobFunc",
		Description: "Test job function",
	})

	scheduler := NewScheduler("")
	scheduler.Start()
	defer scheduler.Stop()

	// 先添加一个任务
	job := Job{
		Id:        "immediate-job",
		Name:      "Test Job",
		FuncName:  "testJobFunc",
		StoreName: "test-store",
		Replace:   true,
		Trigger:   &IntervalTrigger{Interval: 10},
	}
	_, err := scheduler.AddJob(job)
	if err != nil {
		t.Fatalf("AddJob failed: %v", err)
	}

	// 立即执行任务
	err = scheduler.ImmediatelyRunJob(job)
	if err != nil {
		t.Fatalf("ImmediatelyRunJob failed: %v", err)
	}

	// 等待任务执行
	time.Sleep(500 * time.Millisecond)

	// 验证任务被执行
	count, ok := testJobExecCount.Load("immediate-job")
	if !ok || count.(int) == 0 {
		t.Error("Job should have been executed")
	}

	// 测试执行不存在的 store 的任务
	job.StoreName = "non-existent"
	err = scheduler.ImmediatelyRunJob(job)
	if err == nil {
		t.Error("ImmediatelyRunJob should fail for non-existent store")
	}
}

func TestScheduler_SetStore(t *testing.T) {

	scheduler := NewScheduler("")

	// 测试创建 store
	err := scheduler.SetStore("test_store_1")
	if err != nil {
		t.Fatalf("SetStore failed: %v", err)
	}

	// 测试重复创建（应该不会报错）
	err = scheduler.SetStore("test_store_1")
	if err != nil {
		t.Fatalf("SetStore should not fail for existing store: %v", err)
	}

	// 验证 store 存在
	stores := scheduler.GetAllStoreName()
	found := false
	for _, store := range stores {
		if store == "test_store_1" {
			found = true
			break
		}
	}
	if !found {
		t.Error("Store should be created")
	}
}

func TestScheduler_RemoveStore(t *testing.T) {

	RegisterJobsFunc(FuncInfo{
		Func:        testJobFunc,
		Name:        "testJobFunc",
		Description: "Test job function",
	})

	scheduler := NewScheduler("")
	scheduler.Start()
	defer scheduler.Stop()

	// 创建 store 并添加任务
	err := scheduler.SetStore("test-store-remove")
	if err != nil {
		t.Fatalf("SetStore failed: %v", err)
	}

	job := Job{
		Id:        "test-job",
		Name:      "Test Job",
		FuncName:  "testJobFunc",
		StoreName: "test-store-remove",
		Replace:   true,
		Trigger:   &IntervalTrigger{Interval: 10},
	}
	_, err = scheduler.AddJob(job)
	if err != nil {
		t.Fatalf("AddJob failed: %v", err)
	}

	// 删除 store
	err = scheduler.RemoveStore("test-store-remove")
	if err != nil {
		t.Fatalf("RemoveStore failed: %v", err)
	}

	// 验证 store 已删除
	stores := scheduler.GetAllStoreName()
	for _, store := range stores {
		if store == "test-store-remove" {
			t.Error("Store should be removed")
		}
	}
}

func TestScheduler_ConcurrentAccess(t *testing.T) {

	RegisterJobsFunc(FuncInfo{
		Func:        testJobFunc,
		Name:        "testJobFunc",
		Description: "Test job function",
	})

	scheduler := NewScheduler("")
	scheduler.Start()
	defer scheduler.Stop()

	var wg sync.WaitGroup
	concurrency := 10

	// 并发添加任务
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func(idx int) {
			defer wg.Done()
			job := Job{
				Id:        "concurrent-job-" + string(rune('a'+idx)),
				Name:      "Test Job",
				FuncName:  "testJobFunc",
				StoreName: "concurrent-store",
				Replace:   true,
				Trigger:   &IntervalTrigger{Interval: 10},
			}
			_, err := scheduler.AddJob(job)
			if err != nil {
				t.Errorf("Concurrent AddJob failed: %v", err)
			}
		}(i)
	}
	wg.Wait()

	// 验证所有任务都已添加
	jobs, _ := scheduler.GetJobsByStoreName("concurrent-store", 0, 100)
	if len(jobs) != concurrency {
		t.Errorf("Expected %d jobs, got %d", concurrency, len(jobs))
	}
}

func TestScheduler_JobExecution(t *testing.T) {

	testJobExecCount.Store("exec-job", 0)

	RegisterJobsFunc(FuncInfo{
		Func:        testJobFunc,
		Name:        "testJobFunc",
		Description: "Test job function",
	})

	scheduler := NewScheduler("")
	scheduler.Start()
	defer scheduler.Stop()

	// 添加一个短间隔的任务
	job := Job{
		Id:        "exec-job",
		Name:      "Test Job",
		FuncName:  "testJobFunc",
		StoreName: "exec-store",
		Replace:   true,
		Timeout:   5,
		Trigger: &IntervalTrigger{
			Interval: 2, // 2秒间隔
		},
	}
	_, err := scheduler.AddJob(job)
	if err != nil {
		t.Fatalf("AddJob failed: %v", err)
	}

	// 等待任务执行几次
	time.Sleep(6 * time.Second)

	// 验证任务被执行
	count, _ := testJobExecCount.Load("exec-job")
	if count.(int) == 0 {
		t.Error("Job should have been executed at least once")
	}
}

func TestScheduler_JobWithDifferentTriggers(t *testing.T) {

	RegisterJobsFunc(FuncInfo{
		Func:        testJobFunc,
		Name:        "testJobFunc",
		Description: "Test job function",
	})

	scheduler := NewScheduler("")
	scheduler.Start()
	defer scheduler.Stop()

	storeName := "trigger-store"

	// 测试 IntervalTrigger
	intervalJob := Job{
		Id:        "interval-job",
		Name:      "Interval Job",
		FuncName:  "testJobFunc",
		StoreName: storeName,
		Replace:   true,
		Trigger: &IntervalTrigger{
			Interval: 10,
		},
	}
	_, err := scheduler.AddJob(intervalJob)
	if err != nil {
		t.Fatalf("AddJob with IntervalTrigger failed: %v", err)
	}

	// 测试 CronTrigger
	cronJob := Job{
		Id:        "cron-job",
		Name:      "Cron Job",
		FuncName:  "testJobFunc",
		StoreName: storeName,
		Replace:   true,
		Trigger: &CronTrigger{
			CronExpr: "0 * * * * *", // 每分钟
		},
	}
	_, err = scheduler.AddJob(cronJob)
	if err != nil {
		t.Fatalf("AddJob with CronTrigger failed: %v", err)
	}

	// 测试 DateTrigger
	futureTime := time.Now().Add(1 * time.Hour).Format(time.DateTime)
	dateJob := Job{
		Id:        "date-job",
		Name:      "Date Job",
		FuncName:  "testJobFunc",
		StoreName: storeName,
		Replace:   true,
		Trigger: &DateTrigger{
			StartTime: futureTime,
		},
	}
	_, err = scheduler.AddJob(dateJob)
	if err != nil {
		t.Fatalf("AddJob with DateTrigger failed: %v", err)
	}
}
