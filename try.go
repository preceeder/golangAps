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

// PrintStackTrace
func PrintStackTrace(err any) string {
	buf := new(bytes.Buffer)
	if err != nil {
		fmt.Fprintf(buf, "%v \n ", err)
	}
	for i := 1; true; i++ {
		_, file, line, ok := runtime.Caller(i)
		if !ok {
			fmt.Fprintf(buf, "%s:%d \n ", file, line)
			break
		} else {
			fmt.Fprintf(buf, "%s:%d \n ", file, line)
		}
	}
	return buf.String()
}
