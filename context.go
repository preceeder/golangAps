package golangAps

import "context"

type Context struct {
	context.Context
	RequestId string
}

func NewContext() Context {
	return Context{
		context.Background(),
		RandStr(16),
	}
}
