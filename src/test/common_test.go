package test

import (
	"fmt"
	"io/ioutil"
	"log"
	"strings"
	"testing"
	"unicode"
)

func TestSplitString(t *testing.T) {
	ff := func(r rune) bool {
		return !unicode.IsLetter(r)
	}
	s := "hello world from Sheng Jiang"
	words := strings.FieldsFunc(s, ff)
	for _, word := range words {
		fmt.Printf("%v\n", word)
	}
}

func TestReadFile(t *testing.T) {
	fn := "common_test.go"
	content, err := ioutil.ReadFile(fn)
	if err != nil {
		log.Fatalf("read file error: %s", err)
	}
	fmt.Println([]rune(string(content)))
}
