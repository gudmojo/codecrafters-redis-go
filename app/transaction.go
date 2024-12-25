package main

// A connection constitutes a session, and has an optional transaction when the MULTI command is received
type Session struct {
	Transaction *Transaction
}

// Multiple commands are collected into a transaction when the MULTI command is received
type Transaction struct {
	Commands []*Value
}

