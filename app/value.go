package main

import (
	"fmt"
	"strings"
)

// RESP protocol value
type Value struct {
	Typ         string
	Int         int
	Str         string
	Arr         []Value
	PsyncHeader *Value
	PsyncData   *Value
	Bytes       []byte
}

func (v Value) Serialize() []byte {
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
			builder.WriteString(string(x.Serialize()))
		}
		return []byte(fmt.Sprintf("*%d\r\n%s", len(v.Arr), builder.String()))
	case "psync":
		combined := append((*v.PsyncHeader).Serialize(), (*v.PsyncData).Serialize()...)
		return combined
	case "bytes":
		return append([]byte(fmt.Sprintf("$%d\r\n", len(v.Bytes))), v.Bytes...)
	}
	return []byte("")
}

