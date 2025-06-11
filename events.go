package golangAps

import (
	"context"
)

type EventInfo struct {
	Ctx       Context
	EventCode Event
	Job       *Job
	Error     error
	Msg       string
	Result    any // 执行的结果
}

var EventChan chan EventInfo = make(chan EventInfo, 100)

type Event int

const (
	EVENT_JOBSTORE_ADDED   Event = 1 << 0
	EVENT_JOBSTORE_REMOVED Event = 1 << iota
	EVENT_ALL_JOBS_REMOVED Event = 1 << iota
	EVENT_JOB_ADDED        Event = 1 << iota
	EVENT_JOB_REMOVED      Event = 1 << iota
	EVENT_JOB_MODIFIED     Event = 1 << iota
	EVENT_JOB_EXECUTED     Event = 1 << iota
	EVENT_JOB_OVER         Event = 1 << iota
	EVENT_JOB_ERROR        Event = 1 << iota
	EVENT_JOB_MISSED       Event = 1 << iota
	EVENT_MAX_INSTANCE     Event = 1 << iota
)

type EventFunc func(ei EventInfo)

var EventMap = make(map[Event]EventFunc, 0)

func RegisterEvent(eventType Event, ef EventFunc) {
	EventMap[eventType] = ef
}

// StartEventsListen 开启事物监听
func StartEventsListen(ctx context.Context) {
	go func(ctx context.Context) {
		for {
			select {
			case ch := <-EventChan:
				for code, fn := range EventMap {
					if (code & ch.EventCode) == ch.EventCode {
						go func(fn EventFunc, ch EventInfo) {
							defer CatchException(func(err any) {
								DefaultLog.Error(context.Background(), "EventsHandler error", "error", err, "eventInfo", ch, "eventFunc", fn)
							})
							fn(ch)
						}(fn, ch)
					}
				}
			case <-ctx.Done():
				DefaultLog.Info(context.Background(), "Events Listen quit.")
				return
			}
		}
	}(ctx)

}
