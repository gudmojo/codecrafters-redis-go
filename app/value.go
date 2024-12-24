package main

import (
	"fmt"
	"strings"
)

type Value struct {
	Typ         string
	Int         int
	Str         string
	Arr         []Value
	PsyncHeader *Value
	PsyncData   *Value
	Bytes       []byte
}

func Serialize(v Value) []byte {
	switch v.Typ {
	case "bstring":
		if v.Str == "" {
			return []byte("$-1\r\n")
		}
		return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(v.Str), v.Str))
	case "int":
		return []byte(fmt.Sprintf(":%d\r\n", v.Int))
	case "string":
		return []byte(fmt.Sprintf("+%s\r\n", v.Str))
	case "error":
		return []byte(fmt.Sprintf("-%s\r\n", v.Str))
	case "array":
		var builder strings.Builder
		for _, x := range v.Arr {
			builder.WriteString(string(Serialize(x)))
		}
		return []byte(fmt.Sprintf("*%d\r\n%s", len(v.Arr), builder.String()))
	case "psync":
		combined := append(Serialize(*v.PsyncHeader), Serialize(*v.PsyncData)...)
		return combined
	case "bytes":
		return append([]byte(fmt.Sprintf("$%d\r\n", len(v.Bytes))), v.Bytes...)
	}
	return []byte("")
}

