package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"time"
)

type Metadata struct {
	attr []MetadataAttribute
}

type MetadataAttribute struct {
	name  string
	value string
}

type Database struct {
	index         int
	hashTableSize uint
	expirySize    uint
}

func rdbLoadFile(dir, filename string) {
	fn := filename
	if dir != "" {
		fn = dir + "/" + filename
	}
	file, err := os.Open(fn)
	if err != nil {
		return
	}
	defer file.Close()
	bytes, err := io.ReadAll(file)
	if err != nil {
		fmt.Println("Error reading file:", err)
		return
	}
	parseRDB(bytes)
}

func parseRDB(bytes []byte) {
	if string(bytes[0:5]) != "REDIS" {
		panic("Invalid file format")
	}
	i := 5
	rdbVersion := string(bytes[5:9])
	if rdbVersion != "0011" {
		panic("Invalid RDB version: " + rdbVersion)
	}
	i += 4
	res := Metadata{}
	for {
		switch bytes[i] {
		case 0xFA:
			log.Printf(">>>Parsing Metadata: i=%d", i)
			i++
			for !isNewSection(bytes[i], i) {
				log.Printf("ZUZUZU %x", bytes[i])
				r := MetadataAttribute{}
				log.Printf("Parsing MetadataKey: i=%d", i)
				i, r.name = parseStringEncoded(bytes, i)
				log.Printf("Parsed MetadataKey: %s", r.name)
				log.Printf("Parsing MetadataValue: i=%d", i)
				i, r.value = parseStringEncoded(bytes, i)
				log.Printf("Parsed MetadataValue: %s", r.value)
				res.attr = append(res.attr, r)
			}
			log.Printf("===Parsed Metadata: %v", res)
		case 0xFE:
			log.Printf(">>>Parsing DB: i=%d", i)
			i, _ = parseDatabaseSubsection(bytes, i)
			log.Printf("Database subsection parsed. i=%d", i)
			for !isNewSection(bytes[i], i) {
				i = parseObject(bytes, i)
				log.Printf("Object parsed. i=%d", i)
			}
			log.Printf("===Parsed DB: i=%d", i)
		case 0xFF:
			parseEndOfFile(bytes, i)
			return
		}
	}
}

func isNewSection(b byte, i int) bool {
	log.Printf("isNewSection: %x i=%d", b, i)
	return b == 0xFF || b == 0xFE || b == 0xFA
}

func parseEndOfFile(input []byte, i int) {
	if input[i] != 0xFF {
		panic("Invalid end of file marker")
	}
/*	
	checksumWritten := uint64(input[i+1]) | uint64(input[i+2])<<8 | uint64(input[i+3])<<16 | uint64(input[i+4])<<24 | uint64(input[i+5])<<32 | uint64(input[i+6])<<40 | uint64(input[i+7])<<48 | uint64(input[i+8])<<56
	log.Printf("checksumWritten: %d", checksumWritten)
	table := crc64.MakeTable(crc64.ISO)
	checksum := crc64.Checksum(input[:i], table)
	log.Printf("checksumCalculated: %d", checksum)
	if checksum != checksumWritten {
		panic("Checksum mismatch")
	}
		*/
}

func parseObject(input []byte, i int) int {
	var exp time.Time
	if input[i] == 0xFD {
		// timestamp in seconds, 4 bytes little endian (read right-to-left)
		sec := uint32(input[i+1]) | uint32(input[i+2])<<8 | uint32(input[i+3])<<16 | uint32(input[i+4])<<24
		exp = time.Unix(int64(sec), 0)
		i += 5
	} else if input[i] == 0xFC {
		// timestamp in milliseconds, 8 bytes little endian (read right-to-left)
		millis := uint64(input[i+1]) | uint64(input[i+2])<<8 | uint64(input[i+3])<<16 | uint64(input[i+4])<<24 | uint64(input[i+5])<<32 | uint64(input[i+6])<<40 | uint64(input[i+7])<<48 | uint64(input[i+8])<<56
		exp = time.Unix(int64(millis/1000), int64((millis%1000)*1000000))
		i += 9
	}
	valueType := input[i]
	i++
	i, key := parseStringEncoded(input, i)
	if valueType == 0x00 {
		// string type
		var s string
		i, s = parseStringEncoded(input, i)
		GlobalMap[key] = &MapValue{Typ: "string", Str: s, Exp: exp}
	}
	return i
}

func parseDatabaseSubsection(input []byte, i int) (int, Database) {
	var err error
	res := Database{}
	if input[i] != 0xFE {
		panic("Invalid database subsection header")
	}
	i++
	res.index = int(input[i])
	log.Printf("Parsed Database index: %d", res.index)
	i++
	if input[i] != 0xFB {
		panic("Invalid marker for hash table size information follows")
	}
	i++
	i, res.hashTableSize, _, err = parseSizeEncoded(input, i)
	if err != nil {
		panic(err)
	}
	i, res.expirySize, _, err = parseSizeEncoded(input, i)
	if err != nil {
		panic(err)
	}
	log.Printf("Parsed Database: %v", res)
	return i, res
}

// Return values:
// - The new index in the input buffer.
// - The parsed value (size)
// - An error if the size encoding is invalid.
func parseSizeEncoded(input []byte, i int) (int, uint, string, error) {
	// If the first two bits are 0b00:
	// The size is the remaining 6 bits of the byte.
	if input[i]>>6 == 0 {
		x := uint(input[i])
		log.Printf("Parsed size encoded 1: %d i=%d", x, i)
		return i+1, x, "", nil
	}
	// If the first two bits are 0b01:
	// The size is the next 14 bits
	if input[i]>>6 == 1 {
		x := uint(input[i]&0x3F)<<8 | uint(input[i+1])
		log.Printf("Parsed size encoded 2: %d", x)
		return i + 2, x, "", nil
	}
	// If the first two bits are 0b10:
	// Ignore the remaining 6 bits of the first byte.
	// The size is the next 4 bytes, in big-endian (read left-to-right).
	if input[i]>>6 == 2 {
		x := uint(input[i+1])<<24 | uint(input[i+2])<<16 | uint(input[i+3])<<8 | uint(input[i+4])
		log.Printf("Parsed size encoded 3: %d", x)
		return i + 5, x, "", nil
	}
	if input[i]>>6 == 3 {
		if input[i] == 0xC0 {
			// The 0xC0 size indicates the string is an 8-bit integer.
			// In this example, the string is "123".
			// C0 7B
			i++
			x := fmt.Sprintf("%d", input[i])
			i++
			log.Printf("Parsed size encoded 4: %s", x)
			return i, 0, x, nil
		} else if input[i] == 0xC1 {
			// The 0xC1 size indicates the string is a 16-bit integer.
			// The remaining bytes are in little-endian (read right-to-left).
			// In this example, the string is "12345".
			// C1 39 30
			d := uint(input[i+1]) | uint(input[i+2])<<8
			i += 3
			log.Printf("Parsed size encoded 5: %d", d)
			return i, 0, fmt.Sprintf("%d", d), nil
		} else if input[i] == 0xC2 {
			// The 0xC2 size indicates the string is a 32-bit integer.
			// The remaining bytes are in little-endian (read right-to-left),
			// In this example, the string is "1234567".
			// C2 87 D6 12 00
			d := uint(input[i+1]) | uint(input[i+2])<<8 | uint(input[i+3])<<16 | uint(input[i+4])<<24
			i += 5
			log.Printf("Parsed size encoded 6: %d", d)
			return i, 0, fmt.Sprintf("%d", d), nil
		} else if input[i] == 0xC3 {
	        // The 0xC3 size indicates that the string is compressed with the LZF algorithm.
			panic("LZF compression not implemented")
		} else {
			panic("Invalid special encoding: " + fmt.Sprintf("%x i=%d", input[i], i))
		}
	}
	return 0, 0, "", fmt.Errorf("invalid size encoding")
}

func parseStringEncoded(input []byte, i int) (int, string) {
	i, size, str, err := parseSizeEncoded(input, i)
	if err != nil {
		panic(err)
	}
	// Special case for compressed strings when the size is between 0xC0-0xC3
	if str != "" {
		log.Printf("Parsed string encoded: %s", str)
		return i, str
	}
	// Normal strings
	s := string(input[i : i+int(size)])
	return i + int(size), s
}

func intPow(base, exp int) int {
	result := 1
	for exp > 0 {
		if exp%2 == 1 {
			result *= base
		}
		base *= base
		exp /= 2
	}
	return result
}
