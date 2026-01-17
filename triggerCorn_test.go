package golangAps

import (
	"fmt"
	"testing"
	"time"
)

func TestCronTrigger_GetNextRunTime(t *testing.T) {
	ct := CronTrigger{
		CronExpr:     "*/5 * * * *",
		TimeZoneName: "UTC+7",
		StartTime:    time.Now().Add(time.Second * 24).Format("2006-01-02 15:04:05"),
	}
	runTime, err := ct.GetNextRunTime(0, time.Now().Unix())
	if err != nil {
		return
	}
	lc, _ := ParseUtcTimeOffset("UTC+7")
	fmt.Println(time.Unix(runTime, 0).In(lc).Format("2006-01-02 15:04:05"))
}
