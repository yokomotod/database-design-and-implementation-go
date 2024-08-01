package main

import (
	"bufio"
	"fmt"
	"os"
	"reflect"
	"simpledb/parse"
	"strings"
)

func main() {
	fmt.Print("Enter an SQL statement: ")
	for sc := bufio.NewScanner(os.Stdin); sc.Scan(); {
		s := sc.Text()
		p, err := parse.NewParser(s)
		if err != nil {
			fmt.Println(err)
			continue
		}

		if strings.HasPrefix(strings.ToLower(s), "select") {
			query, err := p.Query()
			if err != nil {
				fmt.Println(err)
				continue
			}

			fmt.Printf("%s\n", query)
		} else {
			cmd, err := p.UpdateCmd()
			if err != nil {
				fmt.Println(err)
				continue
			}

			fmt.Printf("%s: %+v\n", reflect.TypeOf(cmd), cmd)
		}

		fmt.Print("Enter an SQL statement: ")
	}
}
