package main

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

func ParseArrayOfBstringValues(buf []byte, i int) (int, *Value, error) {
	Log(fmt.Sprintf("Parsing array of bstring values from %s", buf[i:]))
	i++ // Skip *
	i, c, err := ReadNumber(buf, i)
	if err != nil {
		Log("Error reading number of arguments")
		return i, nil, err
	}
	i += 2 // Skip \r\n
	cmd := make([]Value, c)
	for j := 0; j < c; j++ {
		i += 1 // Skip $
		var bulkLen int
		i, bulkLen, err = ReadNumber(buf, i)
		if err != nil {
			Log(fmt.Sprintf("Error reading bulk length: %v", err))
			return i, nil, err
		}
		i += 2 // Skip \r\n after length indicator
		if bulkLen == -1 {
			cmd[j] = Value{Typ: "bstring", Str: ""} // TODO should this be nil?
		} else {
			value := string(buf[i : i+bulkLen])
			cmd[j] = Value{Typ: "bstring", Str: value}
			i += bulkLen
			i += 2 // Skip \r\n after bstring value
		}
	}
	return i, &Value{Typ: "array", Arr: cmd}, nil
}

func Serialize(v Value) []byte {
	switch v.Typ {
	case "bstring":
		if v.Str == "" {
			return []byte("$-1\r\n")
		}
		return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(v.Str), v.Str))
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

func ReadNumber(s []byte, i int) (int, int, error) {
	Log(fmt.Sprintf("Reading number from %s", s[i:]))
	if i >= len(s) {
		Log(fmt.Sprintf("Index out of range: %d", i))
		return 0, 0, fmt.Errorf("index out of range")
	}
	j := i
	if s[j] == '-' {
		j++
	}
	for j < len(s) && unicode.IsDigit(rune(s[j])) {
		j++
	}
	x, err := strconv.Atoi(string(s[i:j]))
	if err != nil {
		Log(fmt.Sprintf("Error parsing number: %v", err))
		return 0, 0, err
	}
	return j, x, nil
}
