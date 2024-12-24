package main

func execCommand(req *Value, session *Session) Value {
	if session.Transaction == nil {
		return Value{Typ: "error", Str: "ERR EXEC without MULTI"}
	}
	tr := session.Transaction
	session.Transaction = nil
	res := Value{Typ: "array", Arr: []Value{}}
	for _, cmd := range tr.Commands {
		r := HandleRequest(cmd, 0, session)
		res.Arr = append(res.Arr, r)
	}
	return res
}
