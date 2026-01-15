package golangAps

import (
	"bytes"
	"fmt"
	"runtime"
)

func CatchException(handle func(err any)) {
	if err := recover(); err != nil {
		handle(err)
	}
}

// PrintStackTrace 打印堆栈跟踪信息
func PrintStackTrace(err any) string {
	buf := new(bytes.Buffer)
	if err != nil {
		fmt.Fprintf(buf, "Error: %v\n", err)
	}
	buf.WriteString("Stack trace:\n")
	for i := 1; i < 50; i++ { // 限制最多50层调用栈
		pc, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}
		funcName := runtime.FuncForPC(pc).Name()
		fmt.Fprintf(buf, "  %s:%d - %s\n", file, line, funcName)
	}
	return buf.String()
}
