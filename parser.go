package main

import (
	"compress/bzip2"
	"compress/gzip"
	"database/sql"
	"errors"
	"fmt"
	"github.com/tolixx/dirparser"
	"io"
	"log"
	"path"
	"strings"
	"time"
	"tolixx.org/parseres/dbu"
)

type Persons map[string]int

var systems = map[string]int{"b": 0, "a": 1}
var qt = []map[string]int{{"facebook": 0, "instagram": 1, "tiktok": 2, "reddit": 3, "linkedin": 4, "pinterest": 5, "telegram": 6, "tumblr": 7, "patreon": 8},
	{"inurl:facebook.com": 0, "inurl:instagram.com": 1, "inurl:tiktok.com": 2, "inurl:reddit.com": 3, "inurl:linkedin.com": 4, "inurl:pinterest.com": 5, "inurl:telegram.com": 6, "inurl:tumblr.com": 7, "inurl:patreon.com": 8}}

type chainReaderFunc func(reader io.Reader) (io.Reader, error)

func gzipReader(reader io.Reader) (io.Reader, error) {
	log.Printf("Creating new gzip reader")
	return gzip.NewReader(reader)
}

func bzip2Reader(reader io.Reader) (io.Reader, error) {
	log.Printf("Creating new bzip2 reader")
	return bzip2.NewReader(reader), nil
}

type resultParser struct {
	db      *sql.DB
	persons Persons

	badLookups int
	execErrors int
	lines      int
	inserts    int
	files      int

	statPortion int
	chunkSize   int
	separator   string

	start time.Time

	filename string

	Main *dbu.Pair

	readers map[string]chainReaderFunc
}

var errBadLookup error = errors.New("BadLookup")

type optionFunc func(parser *resultParser)

func withStatPortion(value int) optionFunc {
	return func(parser *resultParser) {
		log.Printf("Setting stat portion to %d\n", value)
		parser.statPortion = value
	}
}

func withChunk(value int) optionFunc {
	return func(parser *resultParser) {
		log.Printf("Setting chunk size to %d\n", value)
		parser.chunkSize = value
	}
}

func withSeparator(value string) optionFunc {
	return func(parser *resultParser) {
		parser.separator = value
	}
}

func NewResultParser(db *sql.DB, options ...optionFunc) (*resultParser, error) {
	rp := &resultParser{}
	rp.db = db
	rp.chunkSize = 1000
	rp.statPortion = 10000
	rp.separator = ":::"

	err := rp.loadPersons()
	if err != nil {
		return nil, err
	}

	rp.Main = dbu.NewPair(db)
	rp.start = time.Now()

	for _, opt := range options {
		opt(rp)
	}

	if err := rp.startTransactions(); err != nil {
		return nil, fmt.Errorf("could not start transactions: %v", err)
	}

	rp.readers = map[string]chainReaderFunc{".gz": gzipReader, ".bz2": bzip2Reader}

	return rp, nil
}

func (r *resultParser) initReader(reader io.Reader, filename string) (io.Reader, error) {
	r.files++
	r.filename = path.Base(filename)

	if readChain, ok := r.readers[path.Ext(filename)]; ok {
		return readChain(reader)
	}

	return reader, nil
}

func (r *resultParser) Init(reader io.Reader, filename string) (dirparser.Reader, error) {
	rd, err := r.initReader(reader, filename)
	if err != nil {
		return nil, err
	}
	return dirparser.NewDeepReader(rd, ":::"), nil
}

func (r *resultParser) showStats() {
	sp := float64(r.lines) / time.Now().Sub(r.start).Seconds()
	log.Printf("#%d\tinserted:%d\terrors:%d\tnoPerson:%d\t%.2fL/s %s(%d)", r.lines, r.inserts, r.execErrors, r.badLookups, sp, r.filename, r.files)
}

func (r *resultParser) Parse(record []string) error {
	r.lines++
	if r.lines%r.statPortion == 0 {
		r.showStats()
	}
	person, t := r.parseKey(record[0])

	personID, ok := r.persons[person]
	if !ok {
		r.badLookups++
		return errBadLookup
	}

	se := systems[record[1]]
	_, err := r.Main.Exec(personID, qt[se][t], se, record[2])
	if err != nil {
		r.execErrors++
		return err
	}

	r.inserts++
	if r.inserts%r.chunkSize == 0 {
		r.resetChunk()
	}

	return nil
}

func (r *resultParser) startTransactions() error {
	return r.Main.Start("new_results", "personid", "qt", "se", "url")
}

func (r *resultParser) Close() error {
	return r.Main.Commit()
}

func (r *resultParser) parseKey(str string) (string, string) {
	fqp := strings.Split(str, " ")
	l := len(fqp)

	person := strings.TrimSpace(strings.ToLower(strings.Join(fqp[:l-1], " ")))
	t := fqp[l-1]
	return person, t
}

func (r *resultParser) resetChunk() {
	if err := r.Close(); err != nil {
		log.Printf(" commit error : %v", err)
	}
	r.startTransactions()
}

func (r *resultParser) loadPersons() error {
	q := "SELECT id,name FROM persons"
	r.persons = make(Persons)

	res, err := r.db.Query(q)

	if err != nil {
		return err
	}

	var id int
	var name string

	errs := 0
	lines := 0

	fmt.Printf("LOADING PERSONS [ ")
	for i := 0; i < 89; i++ {
		fmt.Printf(".")
	}
	fmt.Printf("]\r")
	fmt.Printf("LOADING PERSONS [ ")

	for res.Next() {
		err := res.Scan(&id, &name)
		if err != nil {
			errs++
			continue
		}
		r.persons[strings.ToLower(name)] = id
		lines++
		if lines%1000000 == 0 {
			fmt.Printf("o")
		}
	}

	fmt.Printf("\n")
	log.Printf("Persons loaded: %d ok, %d errors", lines, errs)
	return nil
}
