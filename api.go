package golangAps

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"github.com/labstack/echo/v4"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

//go:embed static/*
var staticFiles embed.FS

type ApiTrigger struct {
	StartTime   string `json:"start_time"`
	EndTime     string `json:"end_time"`
	Interval    int64  `json:"interval"`
	UtcTimeZone string `json:"utc_time_zone"`
	Jitter      int64  `json:"jitter"`
	CronExpr    string `json:"cron_expr"`
}
type ApiJob struct {
	// 任务的唯一id.
	Id string `json:"id"` // 一旦设置,不能修改
	// job name
	Name    string     `json:"name"`
	Trigger ApiTrigger `json:"trigger"`
	// 注册函数名
	FuncName string `json:"func_name"` // 必须和注册的函数名一致
	// Arguments for `Func`.
	Args map[string]any `json:"args"`
	// The running timeout of `Func`.
	// ms Default: 20 s
	Timeout int64 `json:"timeout"`
	// jobStoreName
	StoreName   string `json:"store_name"`    // 一旦设置,不能修改
	Replace     bool   `json:"replace"`       // 任务存在是否更新 默认false
	MaxInstance int    `json:"max_instance"`  // 改任务可以同时存在的个数 最少1个, 默认 1
	NextRunTime int64  `json:"next_run_time"` // 查询的时候有这个
}

// 注意  storeName 不能使用   pause | resume | run
func RunAdmin(scheduler *Scheduler, addr string) *echo.Echo {
	e := echo.New()
	e.HideBanner = true
	e.Use(BaseErrorCacheEcho())
	e.Use(GinLoggerEcho())

	// 静态文件嵌入服务
	e.GET("/", func(c echo.Context) error {
		data, err := staticFiles.ReadFile("static/index.html")
		if err != nil {
			return c.String(http.StatusInternalServerError, "Failed to load index.html")
		}
		return c.HTML(http.StatusOK, string(data))
	})

	// 提供静态资源访问，例如 /web/app.js
	e.StaticFS("/web", echo.MustSubFS(staticFiles, "static"))

	e.POST("/job/add", scheduler.EchoAdd)
	e.POST("/job/update", scheduler.EchoUpdateJob)
	e.POST("/job/run", scheduler.EchoImmediatelyRunJob)
	e.GET("/job/:store/:id", scheduler.EchoGetJob)
	e.DELETE("/job/:store/:id", scheduler.EchoDeleteJob)
	e.PUT("/job/pause/:store/:id", scheduler.EchoPauseJob)
	e.PUT("/job/resume/:store/:id", scheduler.EchoResumeJob)
	e.GET("/jobs/all/:store", scheduler.EchoGetJobs)
	e.GET("/jobs/search/:store", scheduler.EchoSearchJobById)
	e.GET("/stores", scheduler.EchoGetStoreNames)
	go func() {
		if err := e.Start(addr); err != nil && err != http.ErrServerClosed {
			e.Logger.Fatal("shutting down the server")
		}
	}()

	return e
}

func (s *Scheduler) EchoAdd(c echo.Context) error {
	var job = ApiJob{}
	if err := c.Bind(&job); err != nil {
		return c.JSON(http.StatusBadRequest, echo.Map{"error": "Invalid job payload", "detail": err.Error()})
	}
	if job.Id == "" || job.StoreName == "" {
		return c.JSON(http.StatusBadRequest, echo.Map{"error": "job.Id and job.StoreName are required"})
	}
	if job.MaxInstance < 0 {
		return c.JSON(http.StatusBadRequest, echo.Map{"error": "job.MaxInstance must be non-negative"})
	}
	if job.Timeout < 0 {
		return c.JSON(http.StatusBadRequest, echo.Map{"error": "job.Timeout must be non-negative"})
	}
	if job.StoreName == "pause" || job.StoreName == "resume" || job.StoreName == "run" {
		return c.JSON(http.StatusBadRequest, echo.Map{"error": "storeName cannot be 'pause', 'resume', or 'run'"})
	}

	saveJob := Job{
		Id:          job.Id,
		Name:        job.Name,
		FuncName:    job.FuncName,
		Args:        job.Args,
		Timeout:     job.Timeout,
		StoreName:   job.StoreName,
		Replace:     job.Replace,
		MaxInstance: job.MaxInstance,
		Trigger:     nil,
	}
	if job.Trigger.CronExpr != "" {
		saveJob.Trigger = &CronTrigger{
			CronExpr:     job.Trigger.CronExpr,
			StartTime:    job.Trigger.StartTime,
			EndTime:      job.Trigger.EndTime,
			Jitter:       job.Trigger.Jitter,
			TimeZoneName: job.Trigger.UtcTimeZone,
		}
	} else if job.Trigger.Interval != 0 {
		saveJob.Trigger = &IntervalTrigger{
			Interval:     job.Trigger.Interval,
			StartTime:    job.Trigger.StartTime,
			EndTime:      job.Trigger.EndTime,
			Jitter:       job.Trigger.Jitter,
			TimeZoneName: job.Trigger.UtcTimeZone,
		}
	} else {
		saveJob.Trigger = &DateTrigger{
			StartTime:    job.Trigger.StartTime,
			Jitter:       job.Trigger.Jitter,
			TimeZoneName: job.Trigger.UtcTimeZone,
		}
	}

	var result Job
	var err error
	result, err = s.AddJob(saveJob)
	if err != nil {
		DefaultLog.Error(c.Request().Context(), "[ERROR]", err)
		return c.JSON(http.StatusInternalServerError, echo.Map{"error": err.Error()})
	}
	return c.JSON(http.StatusOK, result)
}

func (s *Scheduler) EchoUpdateJob(c echo.Context) error {
	var job = ApiJob{}
	if err := c.Bind(&job); err != nil {
		return c.JSON(http.StatusBadRequest, echo.Map{"error": "Invalid job payload", "detail": err.Error()})
	}
	if job.Id == "" || job.StoreName == "" {
		return c.JSON(http.StatusBadRequest, echo.Map{"error": "job.Id and job.StoreName are required"})
	}
	if job.MaxInstance < 0 {
		return c.JSON(http.StatusBadRequest, echo.Map{"error": "job.MaxInstance must be non-negative"})
	}
	if job.Timeout < 0 {
		return c.JSON(http.StatusBadRequest, echo.Map{"error": "job.Timeout must be non-negative"})
	}

	saveJob := Job{
		Id:          job.Id,
		Name:        job.Name,
		FuncName:    job.FuncName,
		Args:        job.Args,
		Timeout:     job.Timeout,
		StoreName:   job.StoreName,
		Replace:     job.Replace,
		MaxInstance: job.MaxInstance,
		Trigger:     nil,
	}
	if job.Trigger.CronExpr != "" {
		saveJob.Trigger = &CronTrigger{
			CronExpr:     job.Trigger.CronExpr,
			StartTime:    job.Trigger.StartTime,
			EndTime:      job.Trigger.EndTime,
			Jitter:       job.Trigger.Jitter,
			TimeZoneName: job.Trigger.UtcTimeZone,
		}
	} else if job.Trigger.Interval != 0 {
		saveJob.Trigger = &IntervalTrigger{
			Interval:     job.Trigger.Interval,
			StartTime:    job.Trigger.StartTime,
			EndTime:      job.Trigger.EndTime,
			Jitter:       job.Trigger.Jitter,
			TimeZoneName: job.Trigger.UtcTimeZone,
		}
	} else {
		saveJob.Trigger = &DateTrigger{
			StartTime:    job.Trigger.StartTime,
			Jitter:       job.Trigger.Jitter,
			TimeZoneName: job.Trigger.UtcTimeZone,
		}
	}

	var result Job
	var err error
	result, err = s.UpdateJob(saveJob)
	if err != nil {
		DefaultLog.Error(c.Request().Context(), "[ERROR]", err)
		return c.JSON(http.StatusInternalServerError, echo.Map{"error": err.Error()})
	}
	return c.JSON(http.StatusOK, result)
}

func (s *Scheduler) EchoGetJob(c echo.Context) error {
	store := c.Param("store")
	id := c.Param("id")
	job, err := s.QueryJob(store, id)
	if err != nil {
		return c.JSON(http.StatusNotFound, echo.Map{"error": err.Error()})
	}
	return c.JSON(http.StatusOK, job)
}

func (s *Scheduler) EchoSearchJobById(c echo.Context) error {
	store := c.Param("store")
	prefix := c.QueryParam("prefix")
	last_id := c.QueryParam("last_id")
	limitStr := c.QueryParam("limit")
	
	limit := 50 // 默认值
	if limitStr != "" {
		var err error
		limit, err = strconv.Atoi(limitStr)
		if err != nil || limit <= 0 {
			return c.JSON(http.StatusBadRequest, echo.Map{"error": "invalid limit parameter, must be a positive integer"})
		}
		if limit > 1000 {
			limit = 1000 // 限制最大值为1000
		}
	}

	jobs, nextId, err := s.SearchJobById(store, prefix, last_id, limit)
	if err != nil {
		return c.JSON(http.StatusNotFound, echo.Map{"error": err.Error()})
	}
	return c.JSON(http.StatusOK, echo.Map{"jobs": jobs, "last_id": nextId})
}

func (s *Scheduler) EchoDeleteJob(c echo.Context) error {
	store := c.Param("store")
	id := c.Param("id")
	err := s.DeleteJob(store, id)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, echo.Map{"error": err.Error()})
	}
	return c.JSON(http.StatusOK, echo.Map{"message": "job deleted"})
}

func (s *Scheduler) EchoPauseJob(c echo.Context) error {
	store := c.Param("store")
	id := c.Param("id")
	job, err := s.PauseJob(store, id)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, echo.Map{"error": err.Error()})
	}
	return c.JSON(http.StatusOK, job)
}

func (s *Scheduler) EchoResumeJob(c echo.Context) error {
	store := c.Param("store")
	id := c.Param("id")
	job, err := s.ResumeJob(store, id)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, echo.Map{"error": err.Error()})
	}
	return c.JSON(http.StatusOK, job)
}

func (s *Scheduler) EchoGetJobs(c echo.Context) error {
	store := c.Param("store")
	offsetStr := c.QueryParam("offset")
	limitStr := c.QueryParam("limit")

	offset := 0
	if offsetStr != "" {
		var err error
		offset, err = strconv.Atoi(offsetStr)
		if err != nil || offset < 0 {
			return c.JSON(http.StatusBadRequest, echo.Map{"error": "invalid offset parameter, must be a non-negative integer"})
		}
	}

	limit := 50 // 默认值
	if limitStr != "" {
		var err error
		limit, err = strconv.Atoi(limitStr)
		if err != nil || limit <= 0 {
			return c.JSON(http.StatusBadRequest, echo.Map{"error": "invalid limit parameter, must be a positive integer"})
		}
		if limit > 1000 {
			limit = 1000 // 限制最大值为1000
		}
	}

	jobs, hasMore := s.GetJobsByStoreName(store, offset, limit)
	return c.JSON(http.StatusOK, echo.Map{
		"jobs":    jobs,
		"has_more": hasMore,
	})
}

func (s *Scheduler) EchoGetStoreNames(c echo.Context) error {
	stores := s.GetAllStoreName()
	return c.JSON(http.StatusOK, stores)
}

func (s *Scheduler) EchoImmediatelyRunJob(c echo.Context) error {
	var job = struct {
		// 任务的唯一id.
		Id string `json:"id"` // 一旦设置,不能修改
		// job name
		Name    string `json:"name"`
		Trigger struct {
			StartTime   string `json:"start_time"`
			EndTime     string `json:"end_time"`
			Interval    int64  `json:"interval"`
			UtcTimeZone string `json:"utc_time_zone"`
			Jitter      int64  `json:"jitter"`
			CronExpr    string `json:"cron_expr"`
		} `json:"trigger"`
		// 注册函数名
		FuncName string `json:"func_name"` // 必须和注册的函数名一致
		// Arguments for `Func`.
		Args map[string]any `json:"args"`
		// The running timeout of `Func`.
		// ms Default: 20 s
		Timeout int64 `json:"timeout"`
		// jobStoreName
		StoreName   string `json:"store_name"`   // 一旦设置,不能修改
		Replace     bool   `json:"replace"`      // 任务存在是否更新 默认false
		MaxInstance int    `json:"max_instance"` // 改任务可以同时存在的个数 最少1个, 默认 1
	}{}
	if err := c.Bind(&job); err != nil {
		return c.JSON(http.StatusBadRequest, echo.Map{"error": "Invalid job payload", "detail": err.Error()})
	}
	if job.Id == "" || job.StoreName == "" {
		return c.JSON(http.StatusBadRequest, echo.Map{"error": "job.Id and job.StoreName are required"})
	}
	if job.MaxInstance < 0 {
		return c.JSON(http.StatusBadRequest, echo.Map{"error": "job.MaxInstance must be non-negative"})
	}
	if job.Timeout < 0 {
		return c.JSON(http.StatusBadRequest, echo.Map{"error": "job.Timeout must be non-negative"})
	}

	runJob := Job{
		Id:          job.Id,
		Name:        job.Name,
		FuncName:    job.FuncName,
		Args:        job.Args,
		Timeout:     job.Timeout,
		StoreName:   job.StoreName,
		Replace:     job.Replace,
		MaxInstance: job.MaxInstance,
		Trigger:     nil, // 执行的时候这个不重要
	}

	err := s.ImmediatelyRunJob(runJob)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, echo.Map{"error": err.Error()})
	}
	return c.JSON(http.StatusOK, echo.Map{"message": "ok"})
}

// BaseErrorCacheEcho Echo 全局兜底 panic 错误中间件
func BaseErrorCacheEcho() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			defer CatchException(func(err any) {
				trace := PrintStackTrace(err)
				DefaultLog.Error(context.Background(), "base panic",
					"err", err,
					"trace", trace,
					"method", c.Request().Method,
					"host", c.Request().Host,
					"uri", c.Request().URL.Path,
				)
				_ = c.JSON(500, echo.Map{"error": "system error"})
			})
			return next(c)
		}
	}
}

// GinLoggerEcho 请求日志记录中间件（Echo 版）
func GinLoggerEcho() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			start := time.Now()
			err := next(c)
			cost := time.Since(start)

			ip := c.Request().Header.Get("X-Forwarded-For")
			params := GetRequestParamsEcho(c)

			DefaultLog.Info(context.Background(), "",
				"method", c.Request().Method,
				"status", c.Response().Status,
				"host", c.Request().Host,
				"ip", ip,
				"params", map[string]any{
					"Body":  params.Body,
					"Query": params.Query,
					"Path":  params.Path,
					"Url":   params.Url,
				},
				"cost", cost.Milliseconds(),
			)

			return err
		}
	}
}

type ParamsData struct {
	Body  any
	Query url.Values
	Url   string
	Path  map[string]string
}

// GetRequestParamsEcho 获取 Echo 请求数据
func GetRequestParamsEcho(c echo.Context) ParamsData {
	var body []byte
	r := c.Request()
	r.ParseForm()

	if cb := c.Get("BodyBytes"); cb != nil {
		if cbb, ok := cb.([]byte); ok {
			body = cbb
		}
	}

	if body == nil {
		bo, err := io.ReadAll(r.Body)
		if err != nil {
			body = []byte("")
		} else {
			body = bo
			// 重设 body 以便后续读取
			r.Body = io.NopCloser(bytes.NewBuffer(body))
			c.Set("BodyBytes", body)
		}
	}

	query := r.Form
	urlp := r.RequestURI
	var bodym any
	if len(body) > 0 {
		if json.Valid(body) {
			_ = json.Unmarshal(body, &bodym)
		} else {
			bodym = string(body)
		}
	}

	pathParams := map[string]string{}
	for _, name := range c.ParamNames() {
		pathParams[name] = c.Param(name)
	}

	return ParamsData{
		Body:  bodym,
		Query: query,
		Url:   urlp,
		Path:  pathParams,
	}
}
