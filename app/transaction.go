package main

type Session struct {
	Transaction *Transaction
}

type Transaction struct {
	Commands []*Value
}

