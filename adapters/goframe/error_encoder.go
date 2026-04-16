package goframe

import "github.com/gogf/gf/v2/errors/gerror"

func ErrorEncoder(err error) (code int32, message string) {
	if err == nil {
		return 0, ""
	}
	return int32(gerror.Code(err).Code()), err.Error()
}
