package dbu

import (
	"database/sql"
	"github.com/lib/pq"
)

type Pair struct {
	db *sql.DB
	Tx *sql.Tx
	St *sql.Stmt
}

func NewPair(db *sql.DB) *Pair {
	p := &Pair{}
	p.db = db
	return p
}

func NewDb(conn string) (*sql.DB, error) {
	db, err := sql.Open("postgres", conn)
	if err != nil {
		return nil, err
	}

	if err = db.Ping(); err != nil {
		return nil, err
	}
	return db, nil
}

func (pair *Pair) Exec(args ...any) (sql.Result, error) {
	return pair.St.Exec(args...)
}
func (pair *Pair) Start(table string, columns ...string) error {
	var err error
	pair.Tx, err = pair.db.Begin()
	if err != nil {
		return err
	}

	pair.St, err = pair.Tx.Prepare(pq.CopyIn(table, columns...))
	return err
}

func (pair *Pair) Commit() error {
	if _, err := pair.St.Exec(); err != nil {
		return err
	}
	if err := pair.St.Close(); err != nil {
		return err
	}
	return pair.Tx.Commit()
}
