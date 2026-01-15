package golangAps

import (
    "fmt"
    "os"
    "path/filepath"
    "runtime"
    "sync/atomic"
    "testing"
    "time"
)

// 基准测试公共辅助：创建临时 scheduler
func newTempScheduler(b *testing.B, name string) (*Scheduler, func()) {
    b.Helper()
    testPath := filepath.Join(os.TempDir(), fmt.Sprintf("bench_%s_%d", name, time.Now().UnixNano()))
    cleanup := func() { _ = os.RemoveAll(testPath) }
    s := NewScheduler(testPath)
    s.Start()
    return s, func() {
        s.Stop()
        cleanup()
    }
}

// 注册一个轻量级执行函数，避免执行成本主导基准
func init() {
    RegisterJobsFunc(FuncInfo{
        Func:        func(j Job) any { return nil },
        Name:        "BenchNoop",
        Description: "noop for benchmarks",
    })
}

// Benchmark: 添加任务（写入 SQLite + 更新索引）
func BenchmarkScheduler_AddJob(b *testing.B) {
    s, done := newTempScheduler(b, "addjob")
    defer done()

    b.ReportAllocs()
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        job := Job{
            Id:        fmt.Sprintf("bench-add-%d", i),
            Name:      "bench-add",
            FuncName:  "BenchNoop",
            StoreName: "bench-store",
            Replace:   true, // 避免重复失败
            Trigger: &IntervalTrigger{
                Interval: 3600, // 降低调度干扰
            },
        }
        if _, err := s.AddJob(job); err != nil {
            b.Fatalf("AddJob error: %v", err)
        }
    }
}

// Benchmark: 更新任务（改写存储 + 索引更新）
func BenchmarkScheduler_UpdateJob(b *testing.B) {
    s, done := newTempScheduler(b, "updatejob")
    defer done()

    // 预置一个任务
    base := Job{
        Id:        "bench-update",
        Name:      "bench-update",
        FuncName:  "BenchNoop",
        StoreName: "bench-store",
        Replace:   true,
        Trigger:   &IntervalTrigger{Interval: 3600},
    }
    if _, err := s.AddJob(base); err != nil {
        b.Fatalf("prep AddJob error: %v", err)
    }

    b.ReportAllocs()
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        base.Timeout = int64(10 + (i % 100))
        if _, err := s.UpdateJob(base); err != nil {
            b.Fatalf("UpdateJob error: %v", err)
        }
    }
}

// Benchmark: 立即执行（仅压测调度通道与提交开销）
func BenchmarkScheduler_ImmediatelyRunJob(b *testing.B) {
    s, done := newTempScheduler(b, "immediately")
    defer done()

    job := Job{
        Id:        "bench-immediate",
        Name:      "bench-immediate",
        FuncName:  "BenchNoop",
        StoreName: "bench-store",
        Replace:   true,
        Trigger:   &IntervalTrigger{Interval: 3600},
    }
    if _, err := s.AddJob(job); err != nil {
        b.Fatalf("prep AddJob error: %v", err)
    }

    b.ReportAllocs()
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        if err := s.ImmediatelyRunJob(job); err != nil {
            b.Fatalf("ImmediatelyRunJob error: %v", err)
        }
    }
}

// Benchmark: 并发添加（替换写），压测锁与存储层并发能力
func BenchmarkScheduler_ParallelAddJob(b *testing.B) {
    s, done := newTempScheduler(b, "paralleladd")
    defer done()

    var ctr int64
    b.SetParallelism(runtime.GOMAXPROCS(0))
    b.ReportAllocs()
    b.ResetTimer()

    b.RunParallel(func(pb *testing.PB) {
        for pb.Next() {
            i := atomic.AddInt64(&ctr, 1)
            job := Job{
                Id:        fmt.Sprintf("bench-par-add-%d", i),
                Name:      "bench-parallel-add",
                FuncName:  "BenchNoop",
                StoreName: "bench-store",
                Replace:   true,
                Trigger:   &IntervalTrigger{Interval: 3600},
            }
            if _, err := s.AddJob(job); err != nil {
                b.Fatalf("AddJob error: %v", err)
            }
        }
    })
}

// Benchmark: 并发更新
func BenchmarkScheduler_ParallelUpdateJob(b *testing.B) {
    s, done := newTempScheduler(b, "parallelupdate")
    defer done()

    // 预置一批任务，避免创建开销影响
    const preN = 1000
    for i := 0; i < preN; i++ {
        job := Job{
            Id:        fmt.Sprintf("bench-par-up-%d", i),
            Name:      "bench-parallel-update",
            FuncName:  "BenchNoop",
            StoreName: "bench-store",
            Replace:   true,
            Trigger:   &IntervalTrigger{Interval: 3600},
        }
        if _, err := s.AddJob(job); err != nil {
            b.Fatalf("prep AddJob error: %v", err)
        }
    }

    var ctr int64
    b.SetParallelism(runtime.GOMAXPROCS(0))
    b.ReportAllocs()
    b.ResetTimer()

    b.RunParallel(func(pb *testing.PB) {
        for pb.Next() {
            i := atomic.AddInt64(&ctr, 1)
            idx := i % preN
            job := Job{
                Id:        fmt.Sprintf("bench-par-up-%d", idx),
                Name:      "bench-parallel-update",
                FuncName:  "BenchNoop",
                StoreName: "bench-store",
                Replace:   true,
                Timeout:   int64(5 + (i % 100)),
                Trigger:   &IntervalTrigger{Interval: 3600},
            }
            if _, err := s.UpdateJob(job); err != nil {
                b.Fatalf("UpdateJob error: %v", err)
            }
        }
    })
}


