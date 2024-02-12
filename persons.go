package main

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
)

type Persons map[string]int

func loadPersons(db *sql.DB) (Persons, error) {
	persons := make(Persons)
	res, err := db.Query("SELECT id,name FROM persons")

	if err != nil {
		return nil, err
	}

	id, name := 0, ""
	errs, lines := 0, 0

	title := "LOADING PERSONS"
	fmt.Printf("%s [ %s ]\r%s [ ", title, strings.Repeat(".", 89), title)

	for res.Next() {
		err := res.Scan(&id, &name)
		if err != nil {
			errs++
			continue
		}
		persons[strings.ToLower(name)] = id
		lines++
		if lines%1000000 == 0 {
			fmt.Printf("o")
		}
	}

	fmt.Printf("\n")
	log.Printf("DONE [ %d ok, %d errors ]", lines, errs)
	return persons, nil
}
