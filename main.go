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

	log.Printf(" => Keywords loaded: %d ok, %d errors", lines, errs)
	return t, nil
}

func startFiller(filename string) error {

	fileinfo, err := os.Stat(filename)
	if err != nil {
		return err
	}

	db, err := openDb(Connection)
	if err != nil {
		return fmt.Errorf("Postgre open error: %v", err)
	}

	defer db.Close()
	persons, err := loadPersons(db)

	if err != nil {
		return err
	}

	if fileinfo.IsDir() {
		return processDirectory(filename, db, persons)
	}

	return processFile(filename, db, persons)

}

func processDirectory(filename string, db *sql.DB, persons map[string]int) error {
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

func processFile(filename string, db *sql.DB, persons map[string]int) error {
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

	if ext == "gz" {
		r, err = gzip.NewReader(reader)
		if err != nil {
			return fmt.Errorf("could not open gz: %v", err)
		}
		log.Printf("Processing as gzip here")
	}

	return fillResults(db, r, persons)
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

func commitAndStartTransaction(db *sql.DB, txn *sql.Tx, stmt *sql.Stmt) (*sql.Tx, *sql.Stmt) {
	commitTransaction(txn, stmt)
	return startTransaction(db)
}

func parseKey(r string) (string, string) {
	fqp := strings.Split(r, " ")
	l := len(fqp)

	person := strings.TrimSpace(strings.ToLower(strings.Join(fqp[:l-1], " ")))
	t := fqp[l-1]
	return person, t
}

func fillResults(db *sql.DB, reader io.Reader, persons map[string]int) error {
	scanner := bufio.NewScanner(reader)
	txn, stmt := startTransaction(db)

	var url string

	number := 0
	valid := 0
	st := time.Now()

	invalidSystems := 0
	badTypes := 0
	badLookups := 0

	for scanner.Scan() {
		number++

		d := strings.Split(scanner.Text(), ":::")
		person, t := parseKey(d[0])
		personid, ok := persons[person]

		if !ok {
			badLookups++
			continue
		}

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
			txn, stmt = commitAndStartTransaction(db, txn, stmt)
			sp := float64(number) / time.Now().Sub(st).Seconds()
			log.Printf("#%d lines, %d valid, speed : %.2f, Bad systems: %d, bad types: %d, bad lookups: %d (%s)", number, valid, sp, invalidSystems, badTypes, badLookups, currentFile)
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
