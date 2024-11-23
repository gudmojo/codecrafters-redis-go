package main

import (
	"fmt"
	"log"
	"strconv"
	"unicode"
)

type Value struct {
	typ string
	str string
}

func parse(buf []byte) ([]Value, error) {
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
		cmd[j] = Value{typ: "bstring", str: value}
		i += bulkLen
	}
	return cmd, nil
}

func serialize(v Value) string {
	switch v.typ {
	case "bstring":
		if v.str == "" {
			return "$-1\r\n"
		}
		return fmt.Sprintf("$%d\r\n%s\r\n", len(v.str), v.str)
	case "string":
		return fmt.Sprintf("+%s\r\n", v.str)
	case "error":
		return fmt.Sprintf("-%s\r\n", v.str)
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

