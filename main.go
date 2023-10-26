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

var systems = map[string]int{"b": 0, "a": 1}

var qt = []map[string]int{{"facebook": 0, "instagram": 1, "tiktok": 2, "reddit": 3, "linkedin": 4, "pinterest": 5, "telegram": 6, "tumblr": 7, "patreon": 8},
	{"inurl:facebook.com": 0, "inurl:instagram.com": 1, "inurl:tiktok.com": 2, "inurl:reddit.com": 3, "inurl:linkedin.com": 4, "inurl:pinterest.com": 5, "inurl:telegram.com": 6, "inurl:tumblr.com": 7, "inurl:patreon.com": 8}}

func main() {
	args := os.Args[1:]
	if len(args) == 0 {
		log.Fatalf("You must specify filename")
	}

	if err := startFiller(args[0]); err != nil {
		log.Fatalf("Filler ends with err: %v", err)
	}

}

func loadPersons(db *sql.DB) (map[string]int, error) {
	q := "SELECT id,name FROM persons"
	t := make(map[string]int)

	res, err := db.Query(q)
	if err != nil {
		return nil, err
	}

	var id int
	var name string

	errs := 0
	lines := 0

	log.Printf("Starting loading persons...")
	for res.Next() {
		err := res.Scan(&id, &name)
		if err != nil {
			errs++
			continue
		}
		t[name] = id
		lines++
		if lines%1000000 == 0 {
			log.Printf("Loaded %d lines", lines)
		}
	}

	log.Printf(" => Keywords loaded: %d ok, %d errors", lines, errs)
	return t, nil
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

	stmt, err := txn.Prepare(pq.CopyIn("new_results", "personid", "qt", "se", "url"))
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
		person, url string
	)

	number := 0
	valid := 0
	st := time.Now()

	invalidSystems := 0
	badTypes := 0
	overLen := 0

	persons, err := loadPersons(db)
	if err != nil {
		return err
	}

	for scanner.Scan() {
		number++

		text := scanner.Text()
		d := strings.Split(text, ":::")

		fq := d[0]
		fqp := strings.Split(fq, " ")
		l := len(fqp)

		person = strings.Title(strings.Join(fqp[:l-1], " "))
		personid, ok := persons[person]

		if !ok {
			log.Printf("Could not found: %s", person)
			continue
		}

		t := fqp[l-1]

		se, ok := systems[d[1]]
		if !ok {
			invalidSystems++
			continue
		}

		qt, ok := qt[se][t]
		if !ok {
			badTypes++
			continue
		}

		url = d[2]
		valid++
		stmt.Exec(personid, qt, se, url)

		if number%300000 == 0 {
			commitTransaction(txn, stmt)
			txn, stmt = startTransaction(db)
			sp := float64(number) / time.Now().Sub(st).Seconds()
			log.Printf("#%d lines, %d valid, speed : %.2f, Bad systems: %d, bad types: %d, overlen :%d", number, valid, sp, invalidSystems, badTypes, overLen)
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
