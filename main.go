package main

import (
	"github.com/jessevdk/go-flags"
	"github.com/tolixx/dirparser"
	"log"
	"tolixx.org/parseres/dbu"
)

type options struct {
	Connection  string `long:"conn" default:"host=185.15.209.153 dbname=parsing user=parser password=N0_1caNw@iT sslmode=disable"`
	StatPortion int    `short:"n" long:"num" default:"10000"`
	ChunkSize   int    `short:"c" long:"chunk" default:"1000"`
	Separator   string `short:"s" long:"separator" default:":::"`
}

var currentFile string

func main() {
	var opts options

	args, err := flags.Parse(&opts)
	if err != nil {
		log.Fatalf("Parse flags error : %v", err)
	}

	if len(args) == 0 {
		log.Fatalf("You must specify filename")
	}

	path := args[0]
	db, err := dbu.NewDb(opts.Connection)
	if err != nil {
		log.Fatalf("Failed connect to DB -> %v", err)
	}

	defer db.Close()

	resParser, err := NewResultParser(db,
		withStatPortion(opts.StatPortion),
		withChunk(opts.ChunkSize),
		withSeparator(opts.Separator))

	if err != nil {
		log.Fatalf("Could not create parser : %v", err)
	}

	if err := dirparser.ParsePath(args[0], resParser); err != nil {
		log.Fatalf("Failed to parse an path %s : %v", path, err)
	}

	log.Printf("Parser completed")
}
