package main

import (
	"fmt"
	"github.com/jessevdk/go-flags"
	"github.com/tolixx/dirparser"
	"log"
	"os"
	"time"
	"tolixx.org/parseres/dbu"
)

type options struct {
	Connection  string `long:"conn" default:"host=127.0.0.1 dbname=parsing user=parser password=N0_1caNw@iT sslmode=disable"`
	StatPortion int    `short:"n" long:"num" default:"10000"`
	ChunkSize   int    `short:"c" long:"chunk" default:"1000"`
	Separator   string `short:"s" long:"separator" default:":::"`
}

func main() {
	start := time.Now()
	if err := startParser(); err != nil {
		log.Printf("Couldn't complete the parser : %v", err)
		os.Exit(1)
	}
	log.Printf("Parser completed in %s ", time.Now().Sub(start))
}

func startParser() error {
	var opts options
	args, err := flags.Parse(&opts)

	if err != nil {
		return err
	}

	if len(args) == 0 {
		return fmt.Errorf("you should specify path")
	}

	path := args[0]
	db, err := dbu.NewDb(opts.Connection)
	if err != nil {
		return fmt.Errorf("failed connect to DB : %v", err)
	}

	defer db.Close()

	resParser, err := NewResultParser(db,
		withStatPortion(opts.StatPortion),
		withChunk(opts.ChunkSize),
		withSeparator(opts.Separator))

	if err != nil {
		return err
	}
	return dirparser.ParsePath(path, resParser)
}
