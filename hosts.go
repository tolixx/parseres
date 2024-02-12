package main

import (
	"database/sql"
	"errors"
	"fmt"
	"golang.org/x/net/publicsuffix"
	"log"
	"strings"
)

type HostMap map[string]int

var errNotFound = errors.New("id not found")

type Hosts struct {
	hosts  HostMap
	db     *sql.DB
	insert *sql.Stmt

	inserts int
}

func NewHosts(db *sql.DB) (*Hosts, error) {
	var err error
	h := &Hosts{hosts: make(HostMap), db: db}
	if err = h.init(); err != nil {
		return nil, err
	}
	h.insert, err = db.Prepare("INSERT INTO hosts(name,tld,suffix) VALUES($1,$2,$3) RETURNING id")
	if err != nil {
		return nil, err
	}
	return h, nil
}

func (h *Hosts) init() error {
	res, err := h.db.Query("SELECT id,name FROM hosts")

	if err != nil {
		return err
	}

	id, name := 0, ""
	errs, lines := 0, 0

	sp := 300000
	tp := 52

	title := "LOADING HOSTS"
	fmt.Printf("%s [ %s ]\r%s [ ", title, strings.Repeat(".", tp), title)

	for res.Next() {
		err := res.Scan(&id, &name)
		if err != nil {
			log.Printf("%v", err)
			return err
			errs++
			continue
		}
		h.hosts[strings.ToLower(name)] = id
		lines++
		if lines%sp == 0 {
			fmt.Printf("o")
		}
	}

	fmt.Printf("\n")
	log.Printf("DONE [ %d ok, %d errors ]", lines, errs)
	return nil
}

func (h *Hosts) getID(name string) (int, error) {
	lower := strings.ToLower(name)
	id, ok := h.hosts[lower]
	if !ok {
		return h.addHost(name)
	}
	return id, nil
}

func (h *Hosts) addHost(domain string) (int, error) {
	suffix, _ := publicsuffix.PublicSuffix(domain)
	tld, err := publicsuffix.EffectiveTLDPlusOne(domain)
	if err != nil {
		return 0, err
	}

	id, err := h.insertHost(domain, tld, suffix)
	if err != nil {
		return 0, err
	}

	h.inserts++
	h.hosts[domain] = id
	return id, nil
}

func (h *Hosts) getInserts() int {
	return h.inserts
}

func (h *Hosts) insertHost(host, tld, suffix string) (int, error) {
	var id int
	err := h.insert.QueryRow(host, tld, suffix).Scan(&id)
	return id, err
}
