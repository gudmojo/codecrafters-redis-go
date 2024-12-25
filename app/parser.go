package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"unicode"
)

// Parses the RESP protocol
type Parser struct {
	reader *bufio.Reader
}

func NewParser(reader *bufio.Reader) *Parser {
	return &Parser{reader: reader}
}

func (r *Parser) ParseArrayOfBstringValues() (int, *Value, error) {
	n, arrayLine, err := r.LineString() // Read *<number of arguments>\r\n
	if err != nil {
		if err == io.EOF {
			return 0, nil, err
		}
		Log(fmt.Sprintf("Error reading array line: %v", err))
		return 0, nil, err
	}
	arrayLen, err := strconv.Atoi(arrayLine[1:])
	if err != nil {
		Log(fmt.Sprintf("Error reading array length: %v", err))
		return 0, nil, err
	}
	cmd := make([]Value, arrayLen)
	for j := 0; j < arrayLen; j++ {
		m, buf, err := r.LineBytes()
		if err != nil {
			Log(fmt.Sprintf("Error reading line: %v", err))
			return 0, nil, err
		}
		n += m
		bulkLen, err := ReadNumber(buf[1:])
		if err != nil {
			Log(fmt.Sprintf("Error reading bulk length: %v", err))
			return 0, nil, err
		}
		if bulkLen == -1 {
			cmd[j] = Value{Typ: "bstring", Str: ""}
		} else {
			m, value, err := r.LineString()
			if err != nil {
				Log(fmt.Sprintf("Error reading value: %v", err))
				return 0, nil, err
			}
			n += m
			cmd[j] = Value{Typ: "bstring", Str: value}
		}
	}
	return n, &Value{Typ: "array", Arr: cmd}, nil
}

func (r *Parser) ReadRdb() ([]byte, error) {
	_, buf, err := r.LineBytes()
	if err != nil {
		Log(fmt.Sprintf("Error reading line: %v", err))
		return nil, err
	}
	bulkLen, err := ReadNumber(buf[1:])
	if err != nil {
		Log(fmt.Sprintf("Error reading bulk length: %v", err))
		return nil, err
	}
	rdb := make([]byte, bulkLen)
	_, err = io.ReadFull(r.reader, rdb)

	if err != nil {
		Log(fmt.Sprintf("Error reading rdb: %v", err))
		return nil, err
	}
	return rdb, nil
}

func (r *Parser) LineString() (int, string, error) {
	line, err := r.reader.ReadString('\n')
	if err != nil {
		return 0, "", err
	}
	return len(line), line[:len(line)-2], nil
}

func (r *Parser) LineBytes() (int, []byte, error) {
	line, err := r.reader.ReadBytes('\n')
	if err != nil {
		return 0, nil, err
	}
	return len(line), line[:len(line)-2], nil
}

func ReadNumber(c []byte) (int, error) {
	if len(c) == 0 {
		return 0, fmt.Errorf("cannot read number from empty string")
	}
	sign := 1
	i := 0
	if c[0] == '-' {
		sign = -1
		i++
	}
	num := 0
	for ; i < len(c); i++ {
		if !unicode.IsDigit(rune(c[i])) {
			break
		}
		num = num*10 + int(c[i]-'0')
	}
	return sign * num, nil
}
