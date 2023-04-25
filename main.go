package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"

	_ "github.com/mattn/go-sqlite3"
)

type respStruct struct {
	Value interface{} `json:"v"`
	Error string      `json:"e"`
}

func toRespBytes(value interface{}, err error) []byte {
	var r respStruct
	r.Value = value
	if err != nil {
		r.Error = err.Error()
	}
	data, _ := json.Marshal(r)
	return data
}

var db *sql.DB

func init() {
	var err error
	db, err = sql.Open("sqlite3", "./storage.db")
	if err != nil {
		log.Fatal(err)
	}
	sqlStmt := `

	create table if not exists  'kvdata' (
	    'id' integer not null primary key,
	    'key' varchar(255) not null,
	    'value' varchar(4096) not null,
	    CONSTRAINT 'kvdata_key_idx' UNIQUE ('key')
	    );

	`
	if err != nil {
		log.Printf("%q: %s\n", err, sqlStmt)
		return
	}
	_, err = db.Exec(sqlStmt)
	if err != nil {
		log.Fatalf("%s: %s\n", err, sqlStmt)
		return
	}
}

func KVGet(key string) (string, error) {
	stmt, err := db.Prepare("select `kvdata`.`value` from `kvdata` where `kvdata`.`key` = ? limit 1")
	if err != nil {
		log.Fatal(err)
		return "", err
	}
	defer stmt.Close()
	var value string
	err = stmt.QueryRow(key).Scan(&value)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		log.Println(err)
		return "", err
	}
	return value, nil
}

func KVSet(key string, value string) error {
	stmt, err := db.Prepare("INSERT OR REPLACE INTO kvdata ('key','value') VALUES (?,?)")
	if err != nil {
		log.Fatal(err)
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec(key, value)

	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}

func KVDelete(key string) error {
	stmt, err := db.Prepare("delete from kvdata where `kvdata`.`key` = ?")
	if err != nil {
		log.Fatal(err)
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec(key)

	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}

func KVScan(key string, count int) (map[string]string, error) {
	stmt, err := db.Prepare("select  `kvdata`.`key`, `kvdata`.`value` from kvdata where `kvdata`.`key` like ? limit ?")
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	defer stmt.Close()

	rows, err := stmt.Query(key+"%", count)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		log.Println(err)
		return nil, err
	}
	ret := map[string]string{}
	for rows.Next() {
		var key, value string
		err := rows.Scan(&key, &value)
		if err != nil {
			log.Println(err)
			return nil, err
		}
		ret[key] = value
	}
	return ret, nil
}

func SqlExec(query string) (map[string]any, error) {
	result, err := db.Exec(query)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		log.Println(err)
		return nil, err
	}

	ret := map[string]any{}
	affected, err := result.RowsAffected()
	if err != nil {
		return nil, err
	}
	ret["RowsAffected"] = affected
	return ret, nil
}

func SqlQuery(query string) ([]map[string]any, error) {
	rows, err := db.Query(query)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		log.Println(err)
		return nil, err
	}
	ret := []map[string]any{}
	for rows.Next() {

		columns, err := rows.Columns()
		if err != nil {
			log.Println(err)
			return nil, err
		}
		rowResult := map[string]any{}
		row := make([]any, len(columns))
		rowPtr := make([]any, len(columns))
		for i := range row {
			rowPtr[i] = &row[i]
		}
		err = rows.Scan(rowPtr...)
		for idx, column := range columns {
			rowResult[column] = row[idx]
		}
		ret = append(ret, rowResult)
	}
	return ret, nil
}
func main() {
	log.SetFlags(0)

	var listenPort = flag.String("listen", "4243", "Listen port.")
	var authKey = flag.String("auth_key", "auth", "authkey")
	flag.Parse()

	if flag.NArg() != 0 {
		flag.Usage()
		log.Fatalf("\nERROR You MUST NOT pass any positional arguments")
	}

	http.HandleFunc("/kv/get", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		data, err := parseBody(r)
		if err != nil {
			w.Write(toRespBytes("", err))
			return
		}
		key := data["key"]
		auth := data["auth_key"]
		if auth != *authKey {
			w.Write(toRespBytes("", errors.New("auth fail")))
			return
		}

		value, err := KVGet(key)
		log.Printf("get %+v value = %s,err = %+v", data, value, err)

		w.Write(toRespBytes(value, err))
	})

	http.HandleFunc("/kv/set", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		data, err := parseBody(r)
		log.Printf("set %+v", data)
		if err != nil {
			w.Write(toRespBytes("", err))
			return
		}

		key := data["key"]
		value := data["value"]
		auth := data["auth_key"]
		if auth != *authKey {
			w.Write(toRespBytes("", errors.New("auth fail")))
			return
		}
		err = KVSet(key, value)
		w.Write(toRespBytes("", err))
	})

	http.HandleFunc("/kv/delete", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		data, err := parseBody(r)
		log.Printf("delete %+v", data)
		if err != nil {
			w.Write(toRespBytes("", err))
			return
		}

		key := data["key"]
		auth := data["auth_key"]
		if auth != *authKey {
			w.Write(toRespBytes("", errors.New("auth fail")))
			return
		}
		err = KVDelete(key)
		w.Write(toRespBytes("", err))
	})

	http.HandleFunc("/kv/scan", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		data, err := parseBody(r)
		log.Printf("scan %+v", data)
		if err != nil {
			w.Write(toRespBytes("", err))
			return
		}

		key := data["key"]
		limit, err := strconv.Atoi(data["limit"])
		if err != nil {
			limit = 10
		}
		auth := data["auth_key"]
		if auth != *authKey {
			w.Write(toRespBytes("", errors.New("auth fail")))
			return
		}
		result, err := KVScan(key, limit)
		w.Write(toRespBytes(result, err))
	})

	http.HandleFunc("/sql/exec", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		data, err := bindSqlExecModel(r)
		log.Printf("exec %+v", data)
		if err != nil {
			w.Write(toRespBytes("", err))
			return
		}

		auth := data.AuthKey
		if auth != *authKey {
			w.Write(toRespBytes("", errors.New("auth fail")))
			return
		}
		result, err := SqlExec(data.Sql)
		w.Write(toRespBytes(result, err))
	})

	http.HandleFunc("/sql/query", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		data, err := bindSqlExecModel(r)
		log.Printf("query %+v", data)
		if err != nil {
			w.Write(toRespBytes("", err))
			return
		}

		auth := data.AuthKey
		if auth != *authKey {
			w.Write(toRespBytes("", errors.New("auth fail")))
			return
		}
		result, err := SqlQuery(data.Sql)
		w.Write(toRespBytes(result, err))
	})

	log.Printf("Listening at http://%s", "0.0.0.0:"+*listenPort)

	httpServer := http.Server{
		Addr: "0.0.0.0:" + *listenPort,
	}

	idleConnectionsClosed := make(chan struct{})
	go func() {
		sigint := make(chan os.Signal, 1)
		signal.Notify(sigint, os.Interrupt)
		<-sigint
		if err := httpServer.Shutdown(context.Background()); err != nil {
			log.Printf("HTTP Server Shutdown Error: %v", err)
		}
		close(idleConnectionsClosed)
	}()

	if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatalf("HTTP server ListenAndServe Error: %v", err)
	}

	<-idleConnectionsClosed

	log.Printf("Bye bye")
}

func parseBody(r *http.Request) (map[string]string, error) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	var data map[string]string
	err = json.Unmarshal(body, &data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func bindSqlExecModel(r *http.Request) (*SqlExecRequest, error) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	var data SqlExecRequest
	err = json.Unmarshal(body, &data)
	if err != nil {
		return nil, err
	}
	return &data, nil
}

func jsonDumps(i interface{}) string {
	bytes, _ := json.Marshal(i)
	return string(bytes)
}
