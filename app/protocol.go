package main

import (
	"fmt"
	"log"
	"strconv"
	"unicode"
	"strings"
)

func Parse(buf []byte) ([]Value, error) {
	i := 1 // Skip *
	i, c, err := ReadNumber(buf, i)
	if err != nil {
		log.Println("Error reading number of arguments")
		return nil, err
	}
	cmd := make([]Value, c)
	for j := 0; j < c; j++ {
		i += 3 // Skip \r\n$
		var bulkLen int
		i, bulkLen, err = ReadNumber(buf, i)
		if err != nil {
			log.Println("Error reading bulk length")
			return nil, err
		}
		i += 2 // Skip \r\n
		value := string(buf[i : i+bulkLen])
		cmd[j] = Value{Typ: "bstring", Str: value}
		i += bulkLen
	}
	return cmd, nil
}

func Serialize(v Value) string {
	switch v.Typ {
	case "bstring":
		if v.Str == "" {
			return "$-1\r\n"
		}
		return fmt.Sprintf("$%d\r\n%s\r\n", len(v.Str), v.Str)
	case "string":
		return fmt.Sprintf("+%s\r\n", v.Str)
	case "error":
		return fmt.Sprintf("-%s\r\n", v.Str)
	case "array":
		var builder strings.Builder 
		for _, x := range v.Arr {
			builder.WriteString(Serialize(x))
		}
		return fmt.Sprintf("*%d\r\n%s", len(v.Arr), builder.String())
	}
	return ""
}

func ReadNumber(s []byte, i int) (int, int, error) {
	j := i
	for ; j < len(s) && unicode.IsDigit(rune(s[j])); {
		j++
	}
	x, err := strconv.Atoi(string(s[i:j]))
	if err != nil {
		log.Println("Error parsing number:", err)
		return 0, 0, err
	}
	return j, int(x), nil
}

