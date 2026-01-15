package golangAps

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// 关闭

type Scheduler struct {
	// Job store
	store *Store
	// It should not be set manually.
	isRunning bool
	cancel    context.CancelFunc
	storeJobs sync.Map // map[string]*StoreJob
	mutexS    sync.RWMutex
}

// NewScheduler 默认创建一个
func NewScheduler(path string) *Scheduler {
	store, err := newStore(path)
	if err != nil {
		panic(err)
	}
	return &Scheduler{
		store:     store,
		mutexS:    sync.RWMutex{},
		storeJobs: sync.Map{},
	}
}

func (s *Scheduler) IsRunning() bool {
	s.mutexS.RLock()
	defer s.mutexS.RUnlock()
	return s.isRunning
}

// Bind the store
func (s *Scheduler) SetStore(storeName string) (err error) {

	s.mutexS.Lock()
	defer s.mutexS.Unlock()
	if _, ok := s.storeJobs.Load(storeName); ok {
		return nil
	}
	// 确保 SQLite 表已创建
	err = s.store.EnsureTable(storeName)
	if err != nil {
		return err
	}
	// 不存在就创建， 存在就不管了
	storeJobs := NewStoreJob(s.store, storeName)
	defer func() {
		EventChan <- EventInfo{
			EventCode: EVENT_JOBSTORE_ADDED,
			Error:     err,
			Msg:       strings.Join([]string{"store name: ", storeName}, ""),
		}
	}()
	err = storeJobs.Start()
	if err != nil {
		return err
	}
	s.storeJobs.Store(storeName, storeJobs)

	return
}

// RemoveStore remove store
func (s *Scheduler) RemoveStore(storeName string) (err error) {
	defer func() {
		EventChan <- EventInfo{
			EventCode: EVENT_JOBSTORE_REMOVED,
			Error:     err,
			Msg:       strings.Join([]string{"store name: ", storeName}, ""),
		}
	}()

	if sj, ok := s.storeJobs.LoadAndDelete(storeName); ok {
		sj.(*StoreJob).Stop()
	}
	// store 都删除了， 就不用管任务是不是在运行了
	return s.store.DeletePartition(storeName)
}

// GetAllStoreName 获取当前所有的 store name
func (s *Scheduler) GetAllStoreName() []string {
	storeNames := s.store.ListPartitions()
	return storeNames
}

// Start scheduler 开启运行
func (s *Scheduler) Start() {
	s.mutexS.Lock()
	defer s.mutexS.Unlock()
	if s.isRunning {
		DefaultLog.Info(context.Background(), "Scheduler is running.")
		return
	}

	s.isRunning = true
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	StartEventsListen(ctx)
	storeNames := s.GetAllStoreName()

	for _, storeName := range storeNames {
		if _, ok := s.storeJobs.Load(storeName); ok {
			// 存在了就不要在启动了
			continue
		}
		sj := NewStoreJob(s.store, storeName)
		if err := sj.Start(); err != nil {
			DefaultLog.Error(context.Background(), "Failed to start StoreJob", "store", storeName, "error", err)
			// 继续处理其他 store，不中断整个启动过程
			continue
		}
		s.storeJobs.Store(storeName, sj)
	}

	DefaultLog.Info(context.Background(), "Scheduler start.")
}

// Stop 停止scheduler
func (s *Scheduler) Stop() {
	s.mutexS.Lock()
	if !s.isRunning {
		s.mutexS.Unlock()
		DefaultLog.Info(context.Background(), "Scheduler has stopped.")
		return
	}
	s.isRunning = false
	s.mutexS.Unlock()

	s.storeJobs.Range(func(k, v interface{}) bool {
		v.(*StoreJob).Stop()
		return true
	})
	err := s.store.Close()
	if err != nil {
		DefaultLog.Error(context.Background(), "Close store failed", "error", err.Error())
	}
	s.cancel()
	DefaultLog.Info(context.Background(), "Scheduler stop.")
}

func (s *Scheduler) AddJob(j Job) (Job, error) {
	var err error
	ctx := NewContext()
	defer func() {
		EventChan <- EventInfo{
			Ctx:       ctx,
			EventCode: EVENT_JOB_ADDED,
			Job:       &j,
			Error:     err,
		}
	}()

	if j.Id == "" {
		err = JobIdError("is can not empty")
		return j, err
	}
	if err = j.Init(); err != nil {
		return Job{}, err
	}

	DefaultLog.Info(ctx, fmt.Sprintf("Scheduler add job `%s`.", j.Name))

	err = s.SetStore(j.StoreName)
	if err != nil {
		DefaultLog.Info(ctx, "Scheduler add store err:", err)
		return Job{}, err
	}
	// 加锁（确保 lockManager 存在）
	lockManager := s.store.ensureLockManager(j.StoreName)
	lock := lockManager.GetLock(j.Id)
	lock.Lock()
	defer lock.Unlock()

	// 存在就替换
	if !j.Replace {
		job, err := s.store.LoadJob(j.StoreName, j.Id)
		if err != nil && !errors.As(err, &JobNotFoundErrorType) {
			return Job{}, err
		}
		if job != nil {
			return Job{}, JobExistsError(j.Id)
		}
	}
	err = s.store.UpdateJob(j.StoreName, &j)
	if err != nil {
		return Job{}, err
	}
	DefaultLog.Info(ctx, "add job", "job", j)

	s.mutexS.RLock()
	isRunning := s.isRunning
	s.mutexS.RUnlock()
	if isRunning {
		if sj, ok := s.storeJobs.Load(j.StoreName); ok {
			select {
			case sj.(*StoreJob).jobChangeChan <- 3:
			default:
				// channel 可能已满或已关闭，忽略
			}
		}
	}
	return j, nil
}

func (s *Scheduler) DeleteJob(storeName string, id string) (err error) {
	err = s.store.RemoveJob(storeName, id)
	if err != nil {
		return err
	}
	s.mutexS.RLock()
	isRunning := s.isRunning
	s.mutexS.RUnlock()
	if isRunning {
		if sj, ok := s.storeJobs.Load(storeName); ok {
			select {
			case sj.(*StoreJob).jobChangeChan <- 4:
			default:
				// channel 可能已满或已关闭，忽略
			}
		}
	}
	return nil
}

func (s *Scheduler) DeleteAllJobs(storeName string) (err error) {
	var storeNames string
	ctx := NewContext()
	defer func() {
		EventChan <- EventInfo{
			Ctx:       ctx,
			EventCode: EVENT_ALL_JOBS_REMOVED,
			Error:     err,
			Msg:       storeNames,
		}
	}()

	DefaultLog.Info(ctx, "delete all jobs.")
	err = s.RemoveStore(storeName)
	if err != nil {
		return err
	}
	return
}

// QueryJob 查询job
func (s *Scheduler) QueryJob(storeName, id string) (*Job, error) {
	job, err := s.store.LoadJob(storeName, id)
	if err != nil {
		return &Job{}, err
	}
	return job, nil
}

func (s *Scheduler) SearchJobById(storeName, prefix, lastJobId string, limit int) ([]Job, string, error) {
	return s.store.SearchJobById(storeName, prefix, lastJobId, limit)
}

// GetJobsByStoreName 获取指定 store 下所有的job
func (s *Scheduler) GetJobsByStoreName(storeName string, offset, limit int) ([]Job, bool) {
	return s.store.GetAllJobs(storeName, offset, limit)
}

// UpdateJob [job.Id, job.StoreName] 不能修改
func (s *Scheduler) UpdateJob(j Job) (Job, error) {
	// 确保 lockManager 存在并加锁
	lockManager := s.store.ensureLockManager(j.StoreName)
	lock := lockManager.GetLock(j.Id)
	lock.Lock()
	defer lock.Unlock()

	var err error
	ctx := NewContext()
	defer func() {
		EventChan <- EventInfo{
			Ctx:       ctx,
			EventCode: EVENT_JOB_MODIFIED,
			Job:       &j,
			Error:     err,
		}
	}()

	err = j.Init()
	if err != nil {
		return j, err
	}

	// get old job
	oldJob, err := s.store.LoadJob(j.StoreName, j.Id)
	if err != nil {
		return Job{}, err
	}
	if j.Status == "" || (j.Status != STATUS_RUNNING && j.Status != STATUS_PAUSED) {
		j.Status = oldJob.Status
	}

	err = s.store.UpdateJob(j.StoreName, &j)
	if err != nil {
		return Job{}, err
	}
	s.mutexS.RLock()
	isRunning := s.isRunning
	s.mutexS.RUnlock()
	if isRunning {
		if sj, ok := s.storeJobs.Load(j.StoreName); ok {
			select {
			case sj.(*StoreJob).jobChangeChan <- 5:
			default:
				// channel 可能已满或已关闭，忽略
			}
		}
	}
	return j, nil
}

func (s *Scheduler) PauseJob(storeName, id string) (Job, error) {
	// 确保 lockManager 存在并加锁
	lockManager := s.store.ensureLockManager(storeName)
	lock := lockManager.GetLock(id)
	lock.Lock()
	defer lock.Unlock()
	ctx := NewContext()
	DefaultLog.Info(ctx, "pause job", "jobId", id)
	// get old job
	job, err := s.store.LoadJob(storeName, id)
	if err != nil {
		return Job{}, err
	}

	job.Status = STATUS_PAUSED
	now := time.Now().Add(time.Hour * 24 * 365 * 100).UTC().Unix()
	job.NextRunTime = now

	err = s.store.UpdateJob(storeName, job)
	if err != nil {
		return Job{}, err
	}
	s.mutexS.RLock()
	isRunning := s.isRunning
	s.mutexS.RUnlock()
	if isRunning {
		if sj, ok := s.storeJobs.Load(job.StoreName); ok {
			select {
			case sj.(*StoreJob).jobChangeChan <- 6:
			default:
				// channel 可能已满或已关闭，忽略
			}
		}
	}
	return *job, nil
}

func (s *Scheduler) ResumeJob(storeName, id string) (Job, error) {
	// 确保 lockManager 存在并加锁
	lockManager := s.store.ensureLockManager(storeName)
	lock := lockManager.GetLock(id)
	lock.Lock()
	defer lock.Unlock()

	ctx := NewContext()
	DefaultLog.Info(ctx, "Scheduler resume job", "jobId", id)
	job, err := s.store.LoadJob(storeName, id)
	if err != nil {
		return Job{}, err
	}

	job.Status = STATUS_RUNNING
	now := time.Now().UTC().Unix()
	job.NextRunTime, _ = job.Trigger.GetNextRunTime(0, now)

	err = s.store.UpdateJob(storeName, job)
	if err != nil {
		return Job{}, err
	}
	s.mutexS.RLock()
	isRunning := s.isRunning
	s.mutexS.RUnlock()
	if isRunning {
		if sj, ok := s.storeJobs.Load(job.StoreName); ok {
			select {
			case sj.(*StoreJob).jobChangeChan <- 7:
			default:
				// channel 可能已满或已关闭，忽略
			}
		}
	}
	return *job, nil
}

// 立即执行任务
func (s *Scheduler) ImmediatelyRunJob(job Job) error {
	sj, ok := s.storeJobs.Load(job.StoreName)
	if !ok {
		return fmt.Errorf("store not found: %s", job.StoreName)
	}
	select {
	case sj.(*StoreJob).immediatelyRunJob <- job:
		return nil
	default:
		return fmt.Errorf("immediately run job channel is full or closed for store: %s", job.StoreName)
	}
}
