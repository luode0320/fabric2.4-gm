package utils

// ErrToString 将and error转换为string。如果错误为nil，则返回字符串 "<clean>"
func ErrToString(err error) string {
	if err != nil {
		return err.Error()
	}

	return "<clean>"
}
