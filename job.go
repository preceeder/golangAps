package golangAps

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"
)

const (
	STATUS_RUNNING = "running"
	STATUS_PAUSED  = "paused"
)

type Job struct {
	// 任务的唯一id.
	Id string `json:"id"` // 一旦设置,不能修改
	// job name
	Name    string  `json:"name"`
	Trigger Trigger `json:"trigger"`
	// 注册函数名
	FuncName string `json:"func_name"` // 必须和注册的函数名一致
	// Arguments for `Func`.
	Args map[string]any `json:"args"`
	// The running timeout of `Func`.
	// ms Default: 20 s
	Timeout int64 `json:"timeout"`
	// Automatic update, not manual setting.  s
	NextRunTime int64 `json:"next_run_time"`
	// Optional: `STATUS_RUNNING` | `STATUS_PAUSED`
	// It should not be set manually.
	Status string `json:"status"`
	// jobStoreName
	StoreName   string `json:"store_name"`   // 一旦设置,不能修改
	Replace     bool   `json:"replace"`      // 任务存在是否更新 默认false
	MaxInstance int    `json:"max_instance"` // 改任务可以同时存在的个数 最少1个, 默认 1
}

// 转成map 用于创建任务时，请求传输map
func (js Job) ToMap() map[string]any {
	return map[string]any{
		"id":           js.Id,
		"name":         js.Name,
		"trigger":      js.Trigger.ToMap(),
		"func_name":    js.FuncName,
		"args":         js.Args,
		"timeout":      js.Timeout,
		"store_name":   js.StoreName,
		"replace":      js.Replace,
		"max_instance": js.MaxInstance,
	}
}

// Initialization functions for each job,
func (j *Job) Init() error {
	j.Status = STATUS_RUNNING

	if j.FuncName == "" {
		return FuncNameNullError(j.Id)
	}

	if j.Timeout == 0 {
		j.Timeout = 20 // 默认20秒
	}

	err := j.Trigger.Init()
	if err != nil {
		return err
	}
	nextRunTime, err := j.Trigger.GetNextRunTime(0, time.Now().UTC().Unix())
	if err != nil {
		return err
	}
	if nextRunTime == 0 {
		return errors.New("endTime can't lt startTime")
	}

	if j.MaxInstance == 0 {
		j.MaxInstance = 1
	}

	j.NextRunTime = nextRunTime

	if err := j.Check(); err != nil {
		return err
	}

	return nil
}

func (j *Job) Check() error {
	// 检查任务函数是否存在
	if _, ok := FuncMap[j.FuncName]; !ok {
		return FuncUnregisteredError(j.FuncName)
	}
	return nil
}

// NextRunTimeHandler 下次执行时间处理, 知道处理为离now时间最短的下一次
func (j *Job) NextRunTimeHandler(ctx Context, nowi int64) (int64, bool, error) {
	nextRunTIme := j.NextRunTime

	var err error
	var IsExpire bool
	jitter := j.Trigger.GetJitterTime()
	if jitter > 0 && nowi-nextRunTIme >= jitter {
		// 本次任务过期, 不执行
		IsExpire = true
	}

	for nextRunTIme != 0 && nextRunTIme <= nowi {
		nextRunTIme, err = j.Trigger.GetNextRunTime(nextRunTIme, nowi)
		if err != nil {
			DefaultLog.Info(ctx, "NextRunTimeHandler", "job", j, "error", err.Error())
			break
		}
	}
	return nextRunTIme, IsExpire, err

}

func (j Job) String() string {
	jobStr, _ := json.Marshal(j)
	return string(jobStr)
}

type FuncInfo struct {
	Func        func(Job) any
	Name        string // 全局唯一函数标志
	Description string // 函数描述
}

var FuncMap = map[string]FuncInfo{
	"ExecuteHttpJob": FuncInfo{
		Func:        ExecuteHttpJob,
		Name:        "ExecuteHttpJob",
		Description: "Execute http job",
	},
	"ExecuteScriptJob": FuncInfo{
		Func:        ExecuteScriptJob,
		Name:        "ExecuteScriptJob",
		Description: "Execute script job",
	},
}

// RegisterJobsFunc 注册全局唯一函数
func RegisterJobsFunc(fis ...FuncInfo) {
	for _, fi := range fis {
		FuncMap[fi.Name] = fi
	}
}

// 通用的执行http 请求的方法
func ExecuteHttpJob(job Job) any {
	// 基本参数提取
	urlStr, _ := job.Args["url"].(string)
	method, _ := job.Args["method"].(string)
	if method == "" {
		method = "GET"
	}
	headers, _ := job.Args["headers"].(map[string]string)
	queryParams, _ := job.Args["query"].(map[string]string)
	bodyParams, _ := job.Args["body"].(map[string]any) // 支持 string 类型 body，可扩展 JSON、form 等

	var bodyBytes []byte = []byte("")
	// 添加 Query 参数
	if len(queryParams) > 0 {
		q := make([]string, 0)
		for k, v := range queryParams {
			q = append(q, fmt.Sprintf("%s=%s", k, v))
		}
		sep := "?"
		if strings.Contains(urlStr, "?") {
			sep = "&"
		}
		urlStr += sep + strings.Join(q, "&")
	}

	if len(bodyParams) > 0 {
		bodyBytes, _ = json.Marshal(bodyParams)
	}

	req, err := http.NewRequest(method, urlStr, strings.NewReader(string(bodyBytes)))
	if err != nil {
		return err
	}

	// 添加 Header
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	// 请求执行
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	_, err = io.ReadAll(resp.Body)
	return err
}

// 通用的执行脚本的任务方法
func ExecuteScriptJob(job Job) any {
	//Args: map[string]any{
	//	   "workdir": "/tmp",
	//    "script":"python -m misd.py",
	//    "args": []string{"-a", "2"},
	//    "timeout": 23,
	//		"env": map[string]string{
	//			"GREETING": "Hello",
	//		},
	//	},
	type ExecuteScript struct {
		WorkDir string
		Script  string
		Args    []string
		Timeout int
		Env     map[string]string
	}
	var jobArgs ExecuteScript = ExecuteScript{}
	marshal, _ := json.Marshal(job.Args)
	err := json.Unmarshal(marshal, &jobArgs)
	if err != nil {
		DefaultLog.Error(context.Background(), "ExecuteScriptJob", "unmarshal job args", err.Error())
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(jobArgs.Timeout)*time.Second)
	defer cancel()

	var args []string
	for _, v := range jobArgs.Args {
		args = append(args, v)
	}

	cmd := exec.CommandContext(ctx, jobArgs.Script, args...)

	// 捕获输出
	var outBuf, errBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf

	// 设置执行目录（可选，根据 job.Args["workdir"] 或默认）
	if jobArgs.WorkDir != "" {
		cmd.Dir = jobArgs.WorkDir
	}

	// 可扩展: 设置环境变量
	if len(jobArgs.Env) > 0 {
		envList := os.Environ()
		for k, v := range jobArgs.Env {
			envList = append(envList, fmt.Sprintf("%s=%s", k, v))
		}
		cmd.Env = envList
	}

	err = cmd.Run()
	return err
}
