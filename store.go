package golangAps

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/gob"
	"fmt"
	"log/slog"
	_ "modernc.org/sqlite"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var DefaultStoreName = "default"
var DefaultDbPath = "./data"

type Store struct {
	db    *jobStorage
	locks sync.Map // map[string]*jobLockManager，使用 sync.Map 避免锁竞争
}

func newStore(path string) (*Store, error) {
	if path == "" {
		path = DefaultDbPath
	}
	store, err := newJobStorage(path)
	if err != nil {
		return nil, err
	}

	return &Store{
		db: store,
	}, nil
}

// 确保 lockManager 存在（如果不存在则创建）
// 使用 sync.Map 的 LoadOrStore 实现无锁的快速检查
func (s *Store) ensureLockManager(storeName string) *jobLockManager {
	// 直接使用 LoadOrStore，确保原子性
	lockManager, _ := s.locks.LoadOrStore(storeName, newJobLockManager())
	return lockManager.(*jobLockManager)
}

// 获取 lockManager（不创建）
func (s *Store) getLockManager(storeName string) *jobLockManager {
	if lockManager, ok := s.locks.Load(storeName); ok {
		return lockManager.(*jobLockManager)
	}
	return nil
}

// EnsureTable 确保指定 store 的 SQLite 表已创建
func (s *Store) EnsureTable(storeName string) error {
	return s.db.ensureTable(storeName)
}

// 获取分区列表
func (s *Store) ListPartitions() []string {
	return s.db.ListPartitions()
}

func (s *Store) UpdateJob(storeName string, j *Job) error {
	jobByte, err := s.StateDump(*j)
	if err != nil {
		slog.ErrorContext(context.Background(), "Jobflush fail", "jobId", j.Id, "err", err.Error())
		return err
	}
	return s.db.SaveJob(storeName, j.Id, jobByte, j.NextRunTime)
}

func (s *Store) RemoveJob(storeName, jobID string) error {
	// 先删除数据库中的 job
	err := s.db.DeleteJob(storeName, jobID)
	if err != nil {
		return err
	}

	// 清理对应的锁，避免内存泄漏
	if lockManager := s.getLockManager(storeName); lockManager != nil {
		lockManager.RemoveLock(jobID)
	}

	return nil
}

func (s *Store) LoadJob(storeName, jobID string) (*Job, error) {
	jb, err := s.db.LoadJob(storeName, jobID)
	if err != nil {
		return nil, err
	}
	return s.StateLoad(jb)
}

func (s *Store) AtomicListAndLock(storeName string, now int64) ([]string, []*sync.Mutex) {
	// 从数据库查询到期的 jobs
	jobIDs, err := s.db.ListDueJobs(storeName, now)
	if err != nil {
		slog.ErrorContext(context.Background(), "ListDueJobs failed", "storeName", storeName, "err", err)
		return nil, nil
	}

	if len(jobIDs) == 0 {
		return nil, nil
	}

	// 获取或创建 lockManager（使用 ensureLockManager 确保存在）
	lockManager := s.ensureLockManager(storeName)

	mutexes := make([]*sync.Mutex, 0, len(jobIDs))
	for _, id := range jobIDs {
		m := lockManager.GetLock(id)
		m.Lock()
		mutexes = append(mutexes, m)
	}
	return jobIDs, mutexes
}

func (s *Store) SearchJobById(storeName, prefix, lastJobId string, limit int) ([]Job, string, error) {
	result := make([]Job, 0, limit)
	jobs, nextJobId, err := s.db.FindJobsByIDPrefixCursor(storeName, prefix, lastJobId, limit)
	if err != nil {
		return nil, "", err
	}
	if len(jobs) > 0 {
		for _, job := range jobs {
			jb, err := s.StateLoad(job)
			if err != nil {
				slog.ErrorContext(context.Background(), "SearchJobById StateLoad failed", "err", err)
				continue // 跳过无效的 job，继续处理其他的
			}
			result = append(result, *jb)
		}
	}
	return result, nextJobId, nil
}

func (s *Store) GetAllJobs(storeName string, offset, limit int) ([]Job, bool) {
	defer CatchException(func(err any) {
		slog.ErrorContext(context.Background(), "error", err)
	})
	jobIds, hasMore, err := s.db.ListAllJobsPaged(storeName, offset, limit)
	if err != nil {
		slog.ErrorContext(context.Background(), "ListAllJobsPaged failed", "storeName", storeName, "err", err)
		return nil, false
	}
	if len(jobIds) == 0 {
		return nil, hasMore
	}
	jobs := make([]Job, 0, len(jobIds))
	for _, jobId := range jobIds {
		job, err := s.LoadJob(storeName, jobId)
		if err != nil {
			slog.ErrorContext(context.Background(), "Job LoadJob fail", "jobId", jobId, "err", err.Error())
			continue
		}
		jobs = append(jobs, *job)
	}
	return jobs, hasMore
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
	// 删除该分区的锁（检查是否存在）
	if lockManager, exists := s.locks.LoadAndDelete(storeName); exists {
		// 清空所有锁
		lockManager.(*jobLockManager).ClearAll()
	}

	// 删除 SQLite 中的表
	return s.db.DropTable(storeName)
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

// RemoveLock 移除指定 job 的锁（用于清理已删除的 job）
func (m *jobLockManager) RemoveLock(jobID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.locks, jobID)
}

// ClearAll 清空所有锁（用于删除 partition 时）
func (m *jobLockManager) ClearAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.locks = make(map[string]*sync.Mutex)
}

type jobStorage struct {
	db            *sql.DB
	createdTables sync.Map // 记录已创建的表，避免重复检查
}

func newJobStorage(path string) (*jobStorage, error) {
	// 确保目录存在
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	// SQLite 数据库文件路径
	dbPath := filepath.Join(path, "jobs.db")

	// 打开 SQLite 数据库
	db, err := sql.Open("sqlite", dbPath+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=foreign_keys(1)")
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// 测试连接
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	jb := &jobStorage{db: db}

	return jb, nil
}

// getTableName 将 store_name 转换为安全的表名
// 表名格式: store_<store_name>，确保表名符合 SQL 标识符规范
func (s *jobStorage) getTableName(storeName string) string {
	// 清理 storeName，只保留字母、数字和下划线
	tableName := strings.Builder{}
	tableName.WriteString("store_")
	for _, r := range storeName {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			tableName.WriteRune(r)
		} else {
			// 将其他字符转换为下划线
			tableName.WriteRune('_')
		}
	}
	return tableName.String()
}

// ensureTable 确保表存在，如果不存在则创建
// 使用 sync.Map 的 LoadOrStore 确保只创建一次，避免并发问题
func (s *jobStorage) ensureTable(storeName string) error {
	// 使用 LoadOrStore 确保只执行一次创建操作
	_, loaded := s.createdTables.LoadOrStore(storeName, true)
	if loaded {
		// 表已经创建过，直接返回
		return nil
	}

	// 第一次创建，执行 SQL
	tableName := s.getTableName(storeName)
	createTableSQL := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		job_id TEXT NOT NULL PRIMARY KEY,
		job_data BLOB NOT NULL,
		next_time INTEGER NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_%s_next_time ON %s(next_time);
	CREATE INDEX IF NOT EXISTS idx_%s_job_id ON %s(job_id);
	`, tableName, tableName, tableName, tableName, tableName)

	_, err := s.db.Exec(createTableSQL)
	if err != nil {
		// 如果创建失败，从缓存中移除，允许重试
		s.createdTables.Delete(storeName)
		return err
	}

	return nil
}

func (s *jobStorage) SaveJob(storeName, jobID string, data []byte, nextTime int64) error {
	if err := s.ensureTable(storeName); err != nil {
		return err
	}
	_, err := s.db.Exec(
		fmt.Sprintf("INSERT INTO %s (job_id, job_data, next_time) VALUES (?, ?, ?) ON CONFLICT(job_id) DO UPDATE SET job_data = ?, next_time = ?", s.getTableName(storeName)),
		jobID, data, nextTime, data, nextTime,
	)
	return err
}

func (s *jobStorage) DeleteJob(storeName, jobID string) error {
	_, err := s.db.Exec(fmt.Sprintf("DELETE FROM %s WHERE job_id = ?", s.getTableName(storeName)), jobID)
	return err
}

func (s *jobStorage) LoadJob(storeName, jobID string) ([]byte, error) {
	var jobData []byte
	err := s.db.QueryRow(fmt.Sprintf("SELECT job_data FROM %s WHERE job_id = ?", s.getTableName(storeName)), jobID).Scan(&jobData)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("job not found: %s/%s", storeName, jobID)
		}
		return nil, err
	}
	return jobData, nil
}

// idPrefix jobid 的前缀
// lastKey 上一次查询最后拿到的jobId
func (s *jobStorage) FindJobsByIDPrefixCursor(storeName, idPrefix, lastKey string, limit int) ([][]byte, string, error) {
	results := make([][]byte, 0, limit)
	var nextCursor string

	var rows *sql.Rows
	var err error

	prefixPattern := idPrefix + "%"
	if lastKey == "" {
		// 第一次查询，使用前缀
		query := fmt.Sprintf("SELECT job_id, job_data FROM %s WHERE job_id LIKE ? ORDER BY job_id LIMIT ?", s.getTableName(storeName))
		rows, err = s.db.Query(query, prefixPattern, limit+1)
	} else {
		// 使用游标继续查询
		query := fmt.Sprintf("SELECT job_id, job_data FROM %s WHERE job_id LIKE ? AND job_id > ? ORDER BY job_id LIMIT ?", s.getTableName(storeName))
		rows, err = s.db.Query(query, prefixPattern, lastKey, limit+1)
	}

	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var jobID string
		var jobData []byte

		if err := rows.Scan(&jobID, &jobData); err != nil {
			return nil, "", err
		}

		// 检查是否超过限制（多查一条用于判断是否有更多数据）
		if count >= limit {
			break
		}

		results = append(results, jobData)
		nextCursor = jobID
		count++
	}

	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	return results, nextCursor, nil
}

// ListDueJobs 查询到期的 jobs
func (s *jobStorage) ListDueJobs(storeName string, now int64) ([]string, error) {
	rows, err := s.db.Query(
		fmt.Sprintf("SELECT job_id FROM %s WHERE next_time <= ? ORDER BY next_time", s.getTableName(storeName)),
		now,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobIDs []string
	for rows.Next() {
		var jobID string
		if err := rows.Scan(&jobID); err != nil {
			return nil, err
		}
		jobIDs = append(jobIDs, jobID)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return jobIDs, nil
}

// NextScheduledTime 获取下一个执行时间
func (s *jobStorage) NextScheduledTime(storeName string) (int64, error) {
	var nextTime sql.NullInt64
	err := s.db.QueryRow(
		fmt.Sprintf("SELECT MIN(next_time) FROM %s WHERE next_time > 0", s.getTableName(storeName)),
	).Scan(&nextTime)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, err
	}
	if !nextTime.Valid {
		return 0, nil
	}
	return nextTime.Int64, nil
}

// ListAllJobsPaged 分页查询所有 jobs
func (s *jobStorage) ListAllJobsPaged(storeName string, offset, limit int) ([]string, bool, error) {
	// 先查询 limit+1 条，用于判断是否有更多数据
	rows, err := s.db.Query(
		fmt.Sprintf("SELECT job_id FROM %s ORDER BY job_id LIMIT ? OFFSET ?", s.getTableName(storeName)),
		limit+1, offset,
	)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()

	var jobIDs []string
	count := 0
	for rows.Next() {
		var jobID string
		if err := rows.Scan(&jobID); err != nil {
			return nil, false, err
		}
		if count < limit {
			jobIDs = append(jobIDs, jobID)
			count++
		} else {
			// 多查了一条，说明还有更多数据，直接返回
			return jobIDs, true, nil
		}
	}

	if err := rows.Err(); err != nil {
		return nil, false, err
	}

	// 如果查询到的数据等于 limit+1，说明还有更多数据
	// 如果查询到的数据少于 limit+1，说明没有更多数据了
	hasMore := count > limit
	return jobIDs, hasMore, nil
}

// ListPartitions 查询所有分区（通过查询所有表名）
func (s *jobStorage) ListPartitions() []string {
	// 查询所有以 store_ 开头的表
	rows, err := s.db.Query(`
		SELECT name FROM sqlite_master 
		WHERE type='table' AND name LIKE 'store_%'
	`)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var partitions []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return partitions
		}
		// 从表名中提取 store_name（去掉 store_ 前缀）
		if strings.HasPrefix(tableName, "store_") {
			storeName := strings.TrimPrefix(tableName, "store_")
			partitions = append(partitions, storeName)
		}
	}

	if err := rows.Err(); err != nil {
		return partitions
	}

	return partitions
}

// DropTable 删除指定 store 的表
func (s *jobStorage) DropTable(storeName string) error {
	_, err := s.db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", s.getTableName(storeName)))
	if err == nil {
		// 删除成功后，从缓存中移除
		s.createdTables.Delete(storeName)
	}
	return err
}

func (s *jobStorage) Close() error {
	return s.db.Close()
}
