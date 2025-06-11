package golangAps

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"github.com/huandu/skiplist"
	"strings"
	"sync"
)

var DefaultStoreName = "default"
var DefaultDbPath = "./data"

type Store struct {
	db         *jobStorage
	partitions map[string]*skipList // 永远存在一个默认分区  default
	locks      map[string]*jobLockManager
	mu         sync.RWMutex
}

func newStore(path string) (*Store, error) {
	if path == "" {
		path = DefaultDbPath
	}
	store, err := newJobStorage(path)
	if err != nil {
		return nil, err
	}
	partitions := make(map[string]*skipList)
	locks := make(map[string]*jobLockManager)
	schedules, err := store.LoadSchedules()
	if err != nil {
		return nil, err
	}
	for storeName, jobs := range schedules {
		sl := newSkipList()
		for jobID, nextTime := range jobs {
			sl.Add(jobID, nextTime)
		}
		partitions[storeName] = sl
		locks[storeName] = newJobLockManager()
	}

	//// 设置默认分区
	//if _, ok := partitions[DefaultStoreName]; !ok {
	//	partitions[DefaultStoreName] = newSkipList()
	//	locks[DefaultStoreName] = newJobLockManager()
	//}

	return &Store{
		db:         store,
		locks:      locks,
		partitions: partitions,
	}, nil
}

// 获取不创建
func (s *Store) getPartitionNoCreate(storeName string) *skipList {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.partitions == nil {
		s.partitions = make(map[string]*skipList)
	}
	if sl, ok := s.partitions[storeName]; ok {
		return sl
	}
	return nil
}

// 可以动态创建分区
func (s *Store) getPartition(storeName string) *skipList {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.partitions == nil {
		s.partitions = make(map[string]*skipList)
	}
	if sl, ok := s.partitions[storeName]; ok {
		return sl
	}
	sl := newSkipList()
	s.partitions[storeName] = sl
	s.locks[storeName] = newJobLockManager()
	return sl
}

// 获取分区列表
func (s *Store) ListPartitions() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	partitions := make([]string, 0, len(s.partitions))
	for key := range s.partitions {
		partitions = append(partitions, key)
	}
	return partitions
}

func (s *Store) UpdateJob(storeName string, j *Job) error {
	jobByte, err := s.StateDump(*j)
	if err != nil {
		DefaultLog.Error(context.Background(), "Jobflush fail", "jobId", j.Id, "err", err.Error())
		return err
	}
	sl := s.getPartitionNoCreate(storeName)
	sl.Update(j.Id, j.NextRunTime)
	return s.db.SaveJob(storeName, j.Id, jobByte, j.NextRunTime)
}

func (s *Store) RemoveJob(storeName, jobID string) error {
	sl := s.getPartitionNoCreate(storeName)
	sl.Remove(jobID)
	return s.db.DeleteJob(storeName, jobID)
}

func (s *Store) LoadJob(storeName, jobID string) (*Job, error) {
	jb, err := s.db.LoadJob(storeName, jobID)
	if err != nil {
		return nil, err
	}
	return s.StateLoad(jb)
}

func (s *Store) AtomicListAndLock(storeName string, now int64) ([]string, []*sync.Mutex) {
	sl := s.getPartitionNoCreate(storeName)
	sl.mu.Lock()
	defer sl.mu.Unlock()
	jobIDs := sl.listDueJobsNoLock(now)
	mutexes := make([]*sync.Mutex, 0, len(jobIDs))
	for _, id := range jobIDs {
		m := s.locks[storeName].GetLock(id)
		m.Lock()
		mutexes = append(mutexes, m)
	}
	return jobIDs, mutexes
}

func (s *Store) SearchJobById(storeName, prefix, lastJobId string, limit int) ([]Job, string, error) {
	result := make([]Job, 0, limit)
	jobs, nextJobId, err := s.db.FindJobsByIDPrefixCursor(storeName, prefix, lastJobId, limit)
	if len(jobs) > 0 {
		for _, job := range jobs {
			jb, _ := s.StateLoad(job)
			result = append(result, *jb)
		}
	}
	return result, nextJobId, err
}

func (s *Store) GetAllJobs(storeName string, offset, limit int) ([]Job, bool) {
	defer CatchException(func(err any) {
		DefaultLog.Error(context.Background(), "error", err)
	})
	if list := s.getPartitionNoCreate(storeName); list != nil {
		jobIds, haMore := list.ListAllJobsPaged(offset, limit)
		if len(jobIds) > 0 {
			jobs := make([]Job, 0, len(jobIds))
			for _, jobId := range jobIds {
				job, err := s.LoadJob(storeName, jobId)
				if err != nil {
					DefaultLog.Error(context.Background(), "Job LoadJob fail", "jobId", jobId, "err", err.Error())
					continue
				}
				jobs = append(jobs, *job)
			}
			return jobs, haMore
		}
	}
	return nil, false
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) StateDump(j Job) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(j)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (s *Store) StateLoad(state []byte) (*Job, error) {
	var j *Job
	buf := bytes.NewBuffer(state)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&j)
	if err != nil {
		return &Job{}, err
	}

	return j, nil
}

func (s *Store) DeletePartition(storeName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 删除内存中的调度器
	delete(s.partitions, storeName)

	// 删除该分区的锁
	for id := range s.locks[storeName].locks {
		if strings.HasPrefix(id, storeName+":") {
			delete(s.locks[storeName].locks, id)
		}
	}

	// 删除 BadgerDB 中的数据
	err := s.db.db.Update(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		prefixes := [][]byte{
			[]byte("job:" + storeName + ":"),
			[]byte("sched:" + storeName + ":"),
		}

		for _, prefix := range prefixes {
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				item := it.Item()
				err := txn.Delete(item.Key())
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
	return err
}

type jobLockManager struct {
	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

func newJobLockManager() *jobLockManager {
	return &jobLockManager{
		locks: make(map[string]*sync.Mutex),
	}
}

func (m *jobLockManager) GetLock(jobID string) *sync.Mutex {
	m.mu.Lock()
	defer m.mu.Unlock()
	if lock, exists := m.locks[jobID]; exists {
		return lock
	}
	lock := &sync.Mutex{}
	m.locks[jobID] = lock
	return lock
}

type skipList struct {
	mu   sync.RWMutex
	list *skiplist.SkipList // key: int64 (nextTime), value: map[jobID]struct{}
}

func newSkipList() *skipList {
	return &skipList{
		list: skiplist.New(skiplist.Int64),
	}
}

func (s *skipList) Add(jobID string, nextTime int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	v := s.list.Get(nextTime)
	if v != nil {
		v.Value.(map[string]struct{})[jobID] = struct{}{}
	} else {
		m := make(map[string]struct{})
		m[jobID] = struct{}{}
		s.list.Set(nextTime, m)
	}
}

func (s *skipList) Remove(jobID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for node := s.list.Front(); node != nil; node = node.Next() {
		jobs := node.Value.(map[string]struct{})
		if _, ok := jobs[jobID]; ok {
			delete(jobs, jobID)
			if len(jobs) == 0 {
				s.list.Remove(node.Key())
			}
			break
		}
	}
}

func (s *skipList) Update(jobID string, nextTime int64) {
	s.Remove(jobID)
	s.Add(jobID, nextTime)
}

func (s *skipList) ListDueJobs(now int64) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []string
	for node := s.list.Front(); node != nil; node = node.Next() {
		t := node.Key().(int64)
		if t > now {
			break
		}
		for jobID := range node.Value.(map[string]struct{}) {
			result = append(result, jobID)
		}
	}
	return result
}

// 无锁版本，内部仅供调用者在外部加锁时使用
func (s *skipList) listDueJobsNoLock(now int64) []string {
	var result []string
	for node := s.list.Front(); node != nil; node = node.Next() {
		t := node.Key().(int64)
		if t > now {
			break
		}
		for jobID := range node.Value.(map[string]struct{}) {
			result = append(result, jobID)
		}
	}
	return result
}

// 获取下一次 执行的时间
func (s *skipList) NextScheduledJobs() (int64, []string) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	node := s.list.Front()
	if node == nil {
		return 0, nil
	}

	nextTime := node.Key().(int64)
	jobIDs := make([]string, 0, len(node.Value.(map[string]struct{})))
	for id := range node.Value.(map[string]struct{}) {
		jobIDs = append(jobIDs, id)
	}
	return nextTime, jobIDs
}

func (s *skipList) ListAllJobsPaged(offset, limit int) ([]string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []string
	skipped := 0
	count := 0
	hasMore := false

	for node := s.list.Front(); node != nil; node = node.Next() {
		for id := range node.Value.(map[string]struct{}) {
			if skipped < offset {
				skipped++
				continue
			}
			if count < limit {
				result = append(result, id)
				count++
			} else {
				hasMore = true
				break
			}
		}
		if hasMore {
			break
		}
	}
	return result, hasMore
}

type jobStorage struct {
	db *badger.DB
}

func newJobStorage(path string) (*jobStorage, error) {
	opts := badger.DefaultOptions(path).WithLogger(nil)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &jobStorage{db: db}, nil
}

func (s *jobStorage) SaveJob(storeName, jobID string, data []byte, nextTime int64) error {
	return s.db.Update(func(txn *badger.Txn) error {
		keyData := fmt.Sprintf("job:%s:%s", storeName, jobID)
		keySched := fmt.Sprintf("sched:%s:%s", storeName, jobID)

		if err := txn.Set([]byte(keyData), data); err != nil {
			return err
		}
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(nextTime))
		return txn.Set([]byte(keySched), buf)
	})
}

func (s *jobStorage) DeleteJob(storeName, jobID string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		if err := txn.Delete([]byte(fmt.Sprintf("job:%s:%s", storeName, jobID))); err != nil {
			return err
		}
		return txn.Delete([]byte(fmt.Sprintf("sched:%s:%s", storeName, jobID)))
	})
}

func (s *jobStorage) LoadJob(storeName, jobID string) ([]byte, error) {
	var valCopy []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(fmt.Sprintf("job:%s:%s", storeName, jobID)))
		if err != nil {
			return err
		}
		valCopy, err = item.ValueCopy(nil)
		return err
	})
	return valCopy, err
}

// idPrefix jobid 的前缀
// lastKey 上一次查询最后拿到的jobId
func (s *jobStorage) FindJobsByIDPrefixCursor(storeName, idPrefix, lastKey string, limit int) ([][]byte, string, error) {
	results := make([][]byte, 0, limit)
	fullPrefix := []byte("job:" + storeName + ":" + idPrefix)

	var nextCursor string

	err := s.db.View(func(txn *badger.Txn) error {
		opt := badger.DefaultIteratorOptions
		opt.PrefetchValues = true
		opt.Prefix = fullPrefix
		it := txn.NewIterator(opt)
		defer it.Close()

		start := fullPrefix
		if lastKey != "" {
			start = []byte("job:" + storeName + ":" + lastKey)
		}

		it.Seek(start)
		if lastKey != "" && it.Valid() && string(it.Item().Key()) == string(start) {
			it.Next() // 跳过当前 lastKey 本身
		}

		count := 0
		for ; it.Valid() && count < limit; it.Next() {
			item := it.Item()
			k := item.Key()
			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			jobID := string(k[len("job:"+storeName+":"):])
			results = append(results, v)
			nextCursor = jobID
			count++
		}

		return nil
	})

	return results, nextCursor, err
}
func (s *jobStorage) LoadSchedules() (map[string]map[string]int64, error) {
	schedules := make(map[string]map[string]int64)
	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewKeyIterator([]byte("sched:"), badger.DefaultIteratorOptions)

		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := string(item.Key())
			parts := strings.SplitN(key, ":", 3)
			if len(parts) != 3 {
				continue
			}
			storeName, jobID := parts[1], parts[2]
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			t := int64(binary.BigEndian.Uint64(val))
			if schedules[storeName] == nil {
				schedules[storeName] = make(map[string]int64)
			}
			schedules[storeName][jobID] = t

		}
		return nil
	})
	return schedules, err
}

func (s *jobStorage) Close() error {
	err := s.db.RunValueLogGC(0.5)
	if err != nil {
		DefaultLog.Error(context.Background(), "Scheduler merge failed", "error", err.Error())
	}
	return s.db.Close()
}
