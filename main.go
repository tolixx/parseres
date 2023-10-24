package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/lib/pq"
)

var Connection = "host=127.0.0.1 dbname=parsing user=parser password=N0_1caNw@iT sslmode=disable"

var bing = map[string]int{"facebook": 0, "instagram": 1, "tiktok": 2, "reddit": 3, "linkedin": 4, "pinterest": 5, "telegram": 6, "tumblr": 7, "patreon": 8}

func main() {
	args := os.Args[1:]
	if len(args) == 0 {
		log.Fatalf("You must specify filename")
	}

	if err := startFiller(args[0]); err != nil {
		log.Fatalf("Filler ends with err: %v", err)
	}

}

func startFiller(filename string) error {
	reader, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("Error opening file %s (%v) ", filename, err)
	}
	defer reader.Close()

	db, err := openDb(Connection)
	if err != nil {
		return fmt.Errorf("Postgre open error: %v", err)
	}

	defer db.Close()

	return fillResults(db, reader)
}

func startTransaction(db *sql.DB) (*sql.Tx, *sql.Stmt) {
	txn, err := db.Begin()

	if err != nil {
		log.Fatalf("Could not start TX")
	}

	stmt, err := txn.Prepare(pq.CopyIn("new_results", "person", "qt", "se", "tp", "url", "snippet"))
	if err != nil {
		log.Fatalf("Could not Prepare %v", err)
	}

	return txn, stmt
}

func commitTransaction(txn *sql.Tx, stmt *sql.Stmt) {
	stmt.Exec()
	stmt.Close()
	err := txn.Commit()
	if err != nil {
		log.Fatalf("Commit: %v", err)
	}
}

func fillResults(db *sql.DB, reader io.Reader) error {
	scanner := bufio.NewScanner(reader)
	txn, stmt := startTransaction(db)

	var (
		person, tp, url, snippet string
		se                       int
	)

	number := 0
	valid := 0
	st := time.Now()

	se = 0

	for scanner.Scan() {
		number++

		text := scanner.Text()
		d := strings.Split(text, ":::")
		if len(d) != 4 {
			log.Printf("Invalid len >> %d", len(d))
			continue
		}

		fq := d[0]
		fqp := strings.Split(fq, " ")
		l := len(fqp)

		person = strings.Join(fqp[:l-2], " ")
		t := fqp[l-1]

		qt, ok := bing[t]
		if !ok {
			log.Printf("Invalid index: %s", t)
			continue
		}

		person = d[0]
		tp = d[1]
		snippet = d[2]

		valid++
		stmt.Exec(person, qt, se, tp, url, snippet)
		if valid%100 == 0 {
			commitTransaction(txn, stmt)
			txn, stmt = startTransaction(db)
			sp := float64(number) / time.Now().Sub(st).Seconds()
			log.Printf("#%d lines, %d valid, speed : %.2f", number, valid, sp)
		}

	}

	commitTransaction(txn, stmt)

	return nil
}

func openDb(conn string) (*sql.DB, error) {
	db, err := sql.Open("postgres", conn)

	if err != nil {
		return nil, err
	}

	if err = db.Ping(); err != nil {
		return nil, err
	}
	return db, nil
}
