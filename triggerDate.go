package golangAps

import (
	"encoding/gob"
	"fmt"
	"time"
)

func init() {
	gob.Register(&DateTrigger{})
}

type DateTrigger struct {
	StartTime    string `json:"start_time"`    // 数据格式 time.DateTime "2006-01-02 15:04:05"
	TimeZoneName string `json:"utc_time_zone"` // 默认UTC
	Jitter       int64  `json:"Jitter"`        // 时间误差, 超过这个误差时间就忽略本次执行,默认 0 表示不管误差, 单位 s time.Second
	runDate      int64
	timeZone     *time.Location
	isInit       bool
}

// 转换成map
func (ct *DateTrigger) ToMap() map[string]any {
	return map[string]any{
		"start_time":    ct.StartTime,
		"utc_time_zone": ct.TimeZoneName,
		"Jitter":        ct.Jitter,
	}
}

// GetLocation 获取时区
func (dt *DateTrigger) GetLocation() (err error) {
	if dt.TimeZoneName == "" {
		dt.TimeZoneName = DefaultTimeZone
	}
	dt.timeZone, err = ParseUtcTimeOffset(dt.TimeZoneName)
	if err != nil {
		return err
	}
	return nil
}

func (dt *DateTrigger) Init() error {
	err := dt.GetLocation()
	if err != nil {
		return err
	}

	if dt.StartTime == "" {
		return fmt.Errorf("StartTime is required for DateTrigger")
	}

	rt, err := time.ParseInLocation(time.DateTime, dt.StartTime, dt.timeZone)
	if err != nil {
		return err
	}

	dt.runDate = rt.UTC().Unix()
	dt.isInit = true

	return nil
}

func (dt *DateTrigger) GetJitterTime() int64 {
	return dt.Jitter
}

// GetNextRunTime
// previousFireTime   s
// now   s
func (dt *DateTrigger) GetNextRunTime(previousFireTime, now int64) (int64, error) {
	if !dt.isInit {
		if err := dt.Init(); err != nil {
			return 0, err
		}
	}
	if previousFireTime == 0 {
		if dt.runDate <= now {
			return 0, nil
		}
		return dt.runDate, nil
	}
	return 0, nil
}
