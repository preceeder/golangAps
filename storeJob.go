package golangAps

import (
	"context"
	"errors"
	"fmt"
	"github.com/panjf2000/ants/v2"
	"os"
	"sync"
	"time"
)

type StoreJob struct {
	store     *Store
	storeName string
	timer     *time.Timer
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
	_ = store.getPartition(storeName)
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

func (b *StoreJob) Start() {
	b.mutexS.Lock()
	b.mutexS.Unlock()
	b.isRunning = true
	b.timer = time.NewTimer(time.Second * 1)
	b.jobChangeChan = make(chan int, 1)
	b.immediatelyRunJob = make(chan Job, 1)
	pool, err := ants.NewPool(1000, ants.WithNonblocking(true))
	if err != nil {
		fmt.Println("init pool failed", "error", err.Error())
		os.Exit(1)
	}
	b.pool = pool
	go b.weakUp()
	go b.run()
}

func (b *StoreJob) Stop() {
	defer b.pool.Release()
	b.isRunning = false
	b.cancel()
	b.isRunning = false
	// 安全退出
	b.waitRun.Wait()
	b.waitJob.Wait()
}

func (b *StoreJob) IsRunning() bool {
	//s.mutexS.RLock()
	//defer s.mutexS.RUnlock()

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

		ch := make(chan error, 1)
		go func() {
			var result any
			defer close(ch)
			defer func() {
				if err := recover(); err != nil {
					DefaultLog.Error(ctx, fmt.Sprintf("Job `%s` run error: %s", j.Name, PrintStackTrace(err)))
					EventChan <- EventInfo{
						Ctx:       ctx,
						EventCode: EVENT_JOB_ERROR,
						Job:       j,
						Error:     err.(error),
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
		case <-ch:
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
func (b *StoreJob) run() func() {
	for {
		select {
		case <-b.timer.C:
			DefaultLog.Info(context.Background(), "执行任务了")
			now := time.Now().UTC()
			nowi := now.Unix()
			jobIds, locks := b.store.AtomicListAndLock(b.storeName, nowi)
			b.loopJob(jobIds, locks, nowi)

			// wait job update completed
			select {
			case b.jobChangeChan <- 2:
			default:
			}
		case job := <-b.immediatelyRunJob:
			// 立即执行任务
			jobLock := b.store.locks[job.StoreName].GetLock(job.Id)
			jobLock.Lock()
			ctx := NewContext()
			err := b.pool.Submit(func() { b.runJob(ctx, &job) })
			if err != nil {
				DefaultLog.Error(ctx, "pool submit _scheduleJob", "store", b.storeName, "error", err.Error(), "job", job)
			}
			jobLock.Unlock()

		case <-b.ctx.Done():
			DefaultLog.Info(context.Background(), "StoreJob run quit.", "store", b.storeName)
			return nil
		}
	}
}

// weakUp 设置定时器
func (b *StoreJob) weakUp() func() {
	for {
		select {
		case <-b.jobChangeChan:
			//fmt.Println("next weak up", b.storeName, su)
			next_wakeup_time := MaxDate.UTC().Unix()
			jobstore_next_run_time, _ := b.store.getPartitionNoCreate(b.storeName).NextScheduledJobs()
			if jobstore_next_run_time != 0 && jobstore_next_run_time < next_wakeup_time {
				next_wakeup_time = jobstore_next_run_time
			}
			now := time.Now().UTC().Unix()
			nextWakeupInterval := next_wakeup_time - now
			if nextWakeupInterval <= 0 {
				nextWakeupInterval = 1
			}
			DefaultLog.Info(context.Background(), fmt.Sprintf("StoreJob next wakeup interval %d", nextWakeupInterval), "store", b.storeName)
			b.timer.Reset(time.Duration(nextWakeupInterval) * time.Second)
		case <-b.ctx.Done():
			DefaultLog.Info(context.Background(), "StoreJob WeakUp quit.", "store", b.storeName)
			return nil
		}
	}
}
