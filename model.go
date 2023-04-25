package main

type AuthInfo struct {
	AuthKey string `json:"auth_key"`
}

type SqlExecRequest struct {
	Sql string `json:"sql"`
	AuthInfo
}
