package golangAps

import (
	"context"
	"errors"
	"fmt"
	"github.com/panjf2000/ants/v2"
	"sync"
	"time"
)

type StoreJob struct {
	store     *Store
	storeName string
	timer     *time.Timer
	timerMu   sync.Mutex // 保护 timer 的并发访问
	pool      *ants.Pool // 默认50,   会被 执行job 和 update job 平分
	// reset timer
	jobChangeChan     chan int
	immediatelyRunJob chan Job // 立即执行任务
	instances         Instance[int]
	ctx               context.Context
	cancel            context.CancelFunc
	isRunning         bool
	mutexS            sync.RWMutex
	waitRun           sync.WaitGroup
	waitJob           sync.WaitGroup
}

func NewStoreJob(store *Store, storeName string) *StoreJob {
	_ = store.ensureLockManager(storeName)
	ctx, cancel := context.WithCancel(context.Background())
	return &StoreJob{
		store:     store,
		storeName: storeName,
		instances: Instance[int]{
			Instances: sync.Map{},
		},
		mutexS:  sync.RWMutex{},
		ctx:     ctx,
		cancel:  cancel,
		waitRun: sync.WaitGroup{},
		waitJob: sync.WaitGroup{},
	}
}

func (b *StoreJob) Start() error {
	b.mutexS.Lock()
	defer b.mutexS.Unlock()
	if b.isRunning {
		return nil
	}
	b.isRunning = true
	b.timer = time.NewTimer(time.Second * 1)
	b.jobChangeChan = make(chan int, 1)
	b.immediatelyRunJob = make(chan Job, 100) // 增加容量以支持高并发立即执行
	pool, err := ants.NewPool(100, ants.WithNonblocking(true))
	if err != nil {
		b.isRunning = false
		return fmt.Errorf("failed to create ants pool: %w", err)
	}
	b.pool = pool
	go b.wakeUp()
	go b.run()
	return nil
}

func (b *StoreJob) Stop() {
	b.mutexS.Lock()
	if !b.isRunning {
		b.mutexS.Unlock()
		return
	}
	b.isRunning = false
	b.mutexS.Unlock()

	b.cancel()
	// 停止 timer
	b.timerMu.Lock()
	if b.timer != nil {
		b.timer.Stop()
	}
	b.timerMu.Unlock()
	// 安全退出
	b.waitRun.Wait()
	b.waitJob.Wait()
	// 关闭 channel（在 goroutine 退出后）
	b.mutexS.Lock()
	if b.jobChangeChan != nil {
		close(b.jobChangeChan)
	}
	if b.immediatelyRunJob != nil {
		close(b.immediatelyRunJob)
	}
	b.mutexS.Unlock()
	b.pool.Release()
}

func (b *StoreJob) IsRunning() bool {
	b.mutexS.RLock()
	defer b.mutexS.RUnlock()
	return b.isRunning
}

func (b *StoreJob) runJob(ctx Context, j *Job) {
	defer func() {
		if err := recover(); err != nil {
			DefaultLog.Error(ctx, fmt.Sprintf("Job `%s` run error: %s", j.Name, err))
		}
	}()

	if f, ok := FuncMap[j.FuncName]; ok {
		defer func() {
			b.instances.Sub(j.Id, 1)
			b.waitJob.Done()
		}()
		b.instances.Add(j.Id, 1)
		b.waitJob.Add(1)

		ctt, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(j.Timeout))
		defer cancel()

		ch := make(chan struct {
			result any
			err    error
		}, 1)
		go func() {
			var result any
			defer func() {
				// 使用select确保不会阻塞，即使ch已经被读取
				select {
				case ch <- struct {
					result any
					err    error
				}{result: result, err: nil}:
				default:
				}
				close(ch)
			}()
			defer func() {
				if err := recover(); err != nil {
					var panicErr error
					if e, ok := err.(error); ok {
						panicErr = e
					} else {
						panicErr = fmt.Errorf("%v", err)
					}
					DefaultLog.Error(ctx, fmt.Sprintf("Job `%s` run error: %s", j.Name, PrintStackTrace(err)))
					EventChan <- EventInfo{
						Ctx:       ctx,
						EventCode: EVENT_JOB_ERROR,
						Job:       j,
						Error:     panicErr,
					}
					select {
					case ch <- struct {
						result any
						err    error
					}{result: nil, err: panicErr}:
					default:
					}
				} else {
					EventChan <- EventInfo{
						Ctx:       ctx,
						EventCode: EVENT_JOB_EXECUTED,
						Job:       j,
						Result:    result,
					}
				}
			}()
			result = f.Func(*j)
		}()

		select {
		case res := <-ch:
			if res.err != nil {
				DefaultLog.Error(ctx, fmt.Sprintf("Job `%s` execution error: %v", j.Id, res.err))
			}
			return
		case <-ctt.Done():
			err := fmt.Sprintf("Job `%s` run timeout", j.Id)
			DefaultLog.Warn(ctx, err)
			EventChan <- EventInfo{
				Ctx:       ctx,
				EventCode: EVENT_JOB_ERROR,
				Job:       j,
				Error:     errors.New(err),
			}
			// 注意：即使超时，goroutine仍会继续运行直到完成
			// 但由于Func不接受context，我们无法真正取消它
			// 这里至少确保错误被正确处理
			return
		}
	} else {
		msg := fmt.Sprintf("Job `%s` Func `%s` unregistered", j.Name, j.FuncName)
		EventChan <- EventInfo{
			Ctx:       ctx,
			EventCode: EVENT_JOB_ERROR,
			Job:       j,
			Error:     errors.New(msg),
		}
		DefaultLog.Warn(ctx, msg)
	}
}

func (b *StoreJob) flushJob(ctx Context, j *Job) {
	// 在这里还需要再次检查任务是否有其他并发问题
	if j.NextRunTime == 0 {
		err := b.store.RemoveJob(b.storeName, j.Id)
		if err != nil {
			DefaultLog.Error(ctx, "Job flush fail", "jobId", j.Id, "err", err.Error())
			return
		}
	} else {
		err := b.store.UpdateJob(b.storeName, j)
		if err != nil {
			DefaultLog.Error(ctx, "Jobflush fail", "jobId", j.Id, "err", err.Error())
			return
		}
	}
	return
}
func (b *StoreJob) loopJob(jobIds []string, locks []*sync.Mutex, nowi int64) {
	taskCtx := NewContext()
	for i, jobID := range jobIds {
		// 加载任务数据
		j, err := b.store.LoadJob(b.storeName, jobID)
		if err != nil {
			// 解除任务锁
			locks[i].Unlock()

			DefaultLog.Error(taskCtx, "Job loop fail", "jobId", jobID, "err", err.Error())
			continue
		}

		if j.NextRunTime <= nowi {

			b.waitRun.Add(1)
			nextRunTime, isExpire, err := j.NextRunTimeHandler(taskCtx, nowi)
			if err != nil {
				DefaultLog.Error(taskCtx, fmt.Sprintf("store: %s, StoreJob calc next run time error: %s", b.storeName, err))
				// 解除任务锁
				locks[i].Unlock()

				b.waitRun.Done()
				continue
			}
			oldRunTime := j.NextRunTime
			j.NextRunTime = nextRunTime
			DefaultLog.Info(taskCtx, "", "store", b.storeName, "jobId", j.Id, "runTime", time.Unix(oldRunTime, 0).Format(time.RFC3339Nano), "next_run_time", time.Unix(j.NextRunTime, 0).Format(time.RFC3339Nano))

			// 先一步更新， 对于后续的 在任务重更新就没啥问题了
			b.flushJob(taskCtx, j)

			if isExpire {
				// job 本次不执行
				EventChan <- EventInfo{
					Ctx:       taskCtx,
					EventCode: EVENT_JOB_MISSED,
					Job:       j,
					Error:     errors.New(fmt.Sprintf("过期, Jitter:%d", j.Trigger.GetJitterTime())),
				}
				DefaultLog.Info(taskCtx, "job expire jump this exec", "store", b.storeName, "jobId", j.Id)
			} else if currentInstance := b.instances.Get(j.Id); currentInstance >= j.MaxInstance {
				// job 本次不执行
				EventChan <- EventInfo{
					Ctx:       taskCtx,
					EventCode: EVENT_MAX_INSTANCE,
					Job:       j,
					Error:     errors.New(fmt.Sprintf("执行的任务数量超限, currentInstance: %d, jobMaxInstance: %d", currentInstance, j.MaxInstance)),
				}
				DefaultLog.Info(taskCtx, "job max instance jump this exec", "store", b.storeName, "jobId", j.Id)
			} else {
				err := b.pool.Submit(func() { b.runJob(taskCtx, j) })
				if err != nil {
					DefaultLog.Error(taskCtx, "pool submit _scheduleJob", "store", b.storeName, "error", err.Error(), "job", j)
				}
			}
			// 解除任务锁
			locks[i].Unlock()

			b.waitRun.Done()
		} else {
			DefaultLog.Warn(taskCtx, "Job nexttime GT Now", "jobId", j.Id, "nexttime", j.NextRunTime, "now", nowi)
			// 解除任务锁
			locks[i].Unlock()

			break
		}
	}
}

// run 调度器
func (b *StoreJob) run() {
	for {
		b.timerMu.Lock()
		timer := b.timer
		b.timerMu.Unlock()

		if timer == nil {
			// 如果 timer 为 nil，说明已经停止，等待退出信号
			<-b.ctx.Done()
			return
		}

		select {
		case <-timer.C:
			DefaultLog.Info(context.Background(), "执行任务了")
			now := time.Now().UTC()
			nowi := now.Unix()
			jobIds, locks := b.store.AtomicListAndLock(b.storeName, nowi)
			b.loopJob(jobIds, locks, nowi)

			// wait job update completed
			// 检查 channel 是否关闭，避免向已关闭的 channel 发送数据导致 panic
			b.mutexS.RLock()
			isRunning := b.isRunning
			b.mutexS.RUnlock()
			if isRunning {
				select {
				case b.jobChangeChan <- 2:
				default:
					// channel 可能已满，忽略
				}
			}
		case job, ok := <-b.immediatelyRunJob:
			if !ok {
				// channel 已关闭
				return
			}
			// 立即执行任务（确保 lockManager 存在）
			lockManager := b.store.ensureLockManager(job.StoreName)
			jobLock := lockManager.GetLock(job.Id)
			jobLock.Lock()
			ctx := NewContext()
			err := b.pool.Submit(func() { 
				defer jobLock.Unlock()
				b.runJob(ctx, &job) 
			})
			if err != nil {
				jobLock.Unlock()
				DefaultLog.Error(ctx, "pool submit _scheduleJob", "store", b.storeName, "error", err.Error(), "job", job)
			}

		case <-b.ctx.Done():
			DefaultLog.Info(context.Background(), "StoreJob run quit.", "store", b.storeName)
			return
		}
	}
}

// wakeUp 设置定时器
func (b *StoreJob) wakeUp() {
	for {
		select {
		case _, ok := <-b.jobChangeChan:
			if !ok {
				// channel 已关闭
				return
			}
			nextWakeupTime := MaxDate.UTC().Unix()
			jobstoreNextRunTime, err := b.store.db.NextScheduledTime(b.storeName)
			if err == nil && jobstoreNextRunTime != 0 && jobstoreNextRunTime < nextWakeupTime {
				nextWakeupTime = jobstoreNextRunTime
			}
			now := time.Now().UTC().Unix()
			nextWakeupInterval := nextWakeupTime - now
			if nextWakeupInterval <= 0 {
				nextWakeupInterval = 1
			}
			DefaultLog.Info(context.Background(), fmt.Sprintf("StoreJob next wakeup interval %d", nextWakeupInterval), "store", b.storeName)
			b.timerMu.Lock()
			if b.timer != nil {
				// Stop 旧的 timer 以避免泄漏
				if !b.timer.Stop() {
					// 如果 timer 已经触发，需要排空 channel
					select {
					case <-b.timer.C:
					default:
					}
				}
				b.timer.Reset(time.Duration(nextWakeupInterval) * time.Second)
			}
			b.timerMu.Unlock()
		case <-b.ctx.Done():
			DefaultLog.Info(context.Background(), "StoreJob wakeUp quit.", "store", b.storeName)
			return
		}
	}
}
