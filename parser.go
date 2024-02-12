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
	"net/url"
	"path"
	"strings"
	"time"
	"tolixx.org/parseres/dbu"
)

var (
	systems = map[string]int{"b": 0, "a": 1}
	qt      = []map[string]int{
		{"facebook": 0, "instagram": 1, "tiktok": 2, "reddit": 3, "linkedin": 4, "pinterest": 5, "telegram": 6, "tumblr": 7, "patreon": 8},
		{"inurl:facebook.com": 0, "inurl:instagram.com": 1, "inurl:tiktok.com": 2, "inurl:reddit.com": 3, "inurl:linkedin.com": 4, "inurl:pinterest.com": 5, "inurl:telegram.com": 6, "inurl:tumblr.com": 7, "inurl:patreon.com": 8}}

	errBadLookup = errors.New("BadLookup")
)

type chainReaderFunc func(reader io.Reader) (io.Reader, error)

type resultParser struct {
	db      *sql.DB
	persons Persons
	hosts   *Hosts

	badLookups int
	execErrors int
	overLen    int
	parseErrs  int
	emptyHosts int
	lines      int
	inserts    int
	files      int
	filename   string

	statPortion int
	chunkSize   int
	separator   string

	start time.Time
	Main  *dbu.Pair

	readers map[string]chainReaderFunc
}

type optionFunc func(parser *resultParser)

func withStatPortion(value int) optionFunc {
	return func(parser *resultParser) {
		parser.statPortion = value
	}
}

func withFileExt(ext string, readerFunc chainReaderFunc) optionFunc {
	return func(parser *resultParser) {
		parser.readers[ext] = readerFunc
	}
}

func withChunk(value int) optionFunc {
	return func(parser *resultParser) {
		parser.chunkSize = value
	}
}

func withSeparator(value string) optionFunc {
	return func(parser *resultParser) {
		parser.separator = value
	}
}

func gzipReader(reader io.Reader) (io.Reader, error) {
	log.Printf("Creating new gzip reader")
	return gzip.NewReader(reader)
}

func bzip2Reader(reader io.Reader) (io.Reader, error) {
	log.Printf("Creating new bzip2 reader")
	return bzip2.NewReader(reader), nil
}

func createParserDefaults(db *sql.DB) *resultParser {
	return &resultParser{
		db:          db,
		chunkSize:   1000,
		statPortion: 10000,
		separator:   ":::",
	}
}

func NewResultParser(db *sql.DB, options ...optionFunc) (*resultParser, error) {
	rp := createParserDefaults(db)
	rp.readers = map[string]chainReaderFunc{".gz": gzipReader, ".bz2": bzip2Reader}

	for _, opt := range options {
		opt(rp)
	}

	var err error
	rp.persons, err = loadPersons(db)

	if err != nil {
		return nil, err
	}

	rp.hosts, err = NewHosts(db)
	if err != nil {
		return nil, err
	}

	rp.Main = dbu.NewPair(db)
	rp.start = time.Now()

	return rp, nil
}

func (r *resultParser) initReader(reader io.Reader, filename string) (io.Reader, error) {
	r.files++
	r.filename = path.Base(filename)

	if readerProxy, ok := r.readers[path.Ext(filename)]; ok {
		return readerProxy(reader)
	}
	return reader, nil
}

func (r *resultParser) Init(reader io.Reader, filename string) (dirparser.Reader, error) {
	rd, err := r.initReader(reader, filename)
	if err != nil {
		log.Printf("%v", err)
		return nil, err
	}

	if err := r.startTransactions(); err != nil {
		log.Printf("could not start transactions: %v", err)
		return nil, fmt.Errorf("could not start transactions: %v", err)
	}
	return dirparser.NewDeepReader(rd, ":::"), nil
}

func (r *resultParser) showStats() {
	sp := float64(r.lines) / time.Now().Sub(r.start).Seconds()
	log.Printf("#%d\tinserted:%d\terrors:%d\tnoPerson:%d\toverLen:%d\tparseErrs:%d\temptyHosts:%d\t%.2fL/s %s(%d)",
		r.lines, r.inserts, r.execErrors, r.badLookups, r.parseErrs, r.emptyHosts, r.overLen, sp, r.filename, r.files)
}

func (r *resultParser) Parse(record []string) error {
	r.lines++
	if r.lines%r.statPortion == 0 {
		r.showStats()
	}
	person, t := r.parseKey(record[0])
	personid, ok := r.persons[person]

	if ok {
		r.badLookups++
		return errBadLookup
	}

	if len(record) > 5 {
		r.overLen++
	}

	u := record[2]
	pu, err := url.Parse(u)
	if err != nil {
		r.parseErrs++
		return nil
	}

	if pu.Host == "" {
		r.emptyHosts++
		return nil
	}

	title := record[3]
	snippet := strings.Join(record[4:], r.separator)

	hostid, err := r.hosts.getID(pu.Host)
	if err != nil {
		return nil
	}

	se := systems[record[1]]
	_, err = r.Main.Exec(personid, qt[se][t], se, u, title, snippet, hostid)

	if err != nil {
		log.Printf("Exec error: %v", err)
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
	//return r.Main.Start("new_results_full", "personid", "qt", "se", "url", "title", "snippet")
	return r.Main.Start("new_results_full", "personid", "qt", "se", "url", "title", "snippet", "hostid")
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
