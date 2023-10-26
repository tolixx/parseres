package main

import (
	"bufio"
	"compress/gzip"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"strings"
	"time"

	"github.com/lib/pq"
)

var Connection = "host=127.0.0.1 dbname=parsing user=parser password=N0_1caNw@iT sslmode=disable"

var systems = map[string]int{"b": 0, "a": 1}
var currentFile string

type Persons map[string]int

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

func loadPersons(db *sql.DB) (Persons, error) {
	q := "SELECT id,name FROM persons"
	t := make(Persons)

	res, err := db.Query(q)
	if err != nil {
		return nil, err
	}

	var id int
	var name string

	errs := 0
	lines := 0

	log.Printf("Starting loading persons...")
	for i := 0; i < 89; i++ {
		fmt.Printf(".")
	}
	fmt.Printf("\r")

	for res.Next() {
		err := res.Scan(&id, &name)
		if err != nil {
			errs++
			continue
		}
		t[strings.ToLower(name)] = id
		lines++
		if lines%1000000 == 0 {
			fmt.Printf("o")
		}
	}

	fmt.Printf("\n")
	log.Printf(" => Keywords loaded: %d ok, %d errors", lines, errs)
	return t, nil
}

func startFiller(filename string) error {

	info, err := os.Stat(filename)
	if err != nil {
		return err
	}

	db, err := openDb(Connection)
	if err != nil {
		return fmt.Errorf("postgre open error: %v", err)
	}

	defer db.Close()

	persons, err := loadPersons(db)

	if err != nil {
		return err
	}

	if info.IsDir() {
		return processDirectory(filename, db, persons)
	}

	return processFile(filename, db, persons)
}

func processDirectory(filename string, db *sql.DB, persons Persons) error {
	d, err := os.Open(filename)
	if err != nil {
		return err
	}

	log.Printf("starting to process directory : %s", filename)
	files, err := d.ReadDir(0)
	if err != nil {
		return err
	}

	for index := range files {
		file := files[index]
		name := file.Name()

		if name[0] == '.' {
			continue
		}

		if err := processFile(filename+"/"+name, db, persons); err != nil {
			log.Printf("Failed to process : %s", name)
		}
	}

	return nil
}

func processFile(filename string, db *sql.DB, persons Persons) error {
	log.Printf("Starting to process file: %s", filename)
	reader, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("Error opening file %s (%v) ", filename, err)
	}
	defer reader.Close()

	currentFile = filename

	var r io.Reader = reader
	ext := path.Ext(filename)

	log.Printf("file extension is: %s", ext)

	if ext == ".gz" {
		r, err = gzip.NewReader(reader)
		if err != nil {
			return fmt.Errorf("could not open gz: %v", err)
		}
		log.Printf("Processing as gzip here")
	}

	return fillResults(db, r, persons)
}

func startTransaction(db *sql.DB) (*sql.Tx, *sql.Stmt, error) {
	txn, err := db.Begin()

	if err != nil {
		return nil, nil, err
	}

	stmt, err := txn.Prepare(pq.CopyIn("new_results", "personid", "qt", "se", "url"))
	if err != nil {
		return nil, nil, err
	}

	return txn, stmt, nil
}

func commitTransaction(txn *sql.Tx, stmt *sql.Stmt) error {
	if _, err := stmt.Exec(); err != nil {
		return err
	}
	if err := stmt.Close(); err != nil {
		return err
	}
	return txn.Commit()

}

func commitAndStartTransaction(db *sql.DB, txn *sql.Tx, stmt *sql.Stmt) (*sql.Tx, *sql.Stmt, error) {
	if err := commitTransaction(txn, stmt); err != nil {
		return nil, nil, err
	}
	return startTransaction(db)
}

func parseKey(r string) (string, string) {
	fqp := strings.Split(r, " ")
	l := len(fqp)

	person := strings.TrimSpace(strings.ToLower(strings.Join(fqp[:l-1], " ")))
	t := fqp[l-1]
	return person, t
}

func fillResults(db *sql.DB, reader io.Reader, persons Persons) error {

	var number, valid, errors, badLookups int

	st := time.Now()
	scanner := bufio.NewScanner(reader)
	txn, stmt, err := startTransaction(db)

	if err != nil {
		return fmt.Errorf("couldn't start transaction: %v", err)
	}

	for scanner.Scan() {
		number++

		d := strings.Split(scanner.Text(), ":::")
		person, t := parseKey(d[0])

		personID, ok := persons[person]
		if !ok {
			badLookups++
			continue
		}

		se := systems[d[1]]
		valid++

		if _, err := stmt.Exec(personID, qt[se][t], se, d[2]); err != nil {
			errors++
		}

		if number%300000 == 0 {
			txn, stmt, err = commitAndStartTransaction(db, txn, stmt)
			if err != nil {
				return fmt.Errorf("couldn't start transaction: %v", err)
			}
			sp := float64(number) / time.Now().Sub(st).Seconds()
			log.Printf("#%d lines, %d valid, speed : %.2f, bad lookups: %d, errors: %d (%s)", number, valid, sp, badLookups, errors, currentFile)
		}
	}

	return commitTransaction(txn, stmt)
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
