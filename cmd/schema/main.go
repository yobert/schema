package main

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/jackc/pgx/stdlib"
	"github.com/yobert/schema"
	"os"
	"path"
	"time"
)

func main() {

	start := time.Now()

	var (
		user    string
		pass    string
		host    string
		port    int
		name    string
		search  string
		dry     bool
		verbose bool
	)

	flag.StringVar(&user, "u", "", "User")
	flag.StringVar(&pass, "p", "", "Password")
	flag.StringVar(&host, "h", "localhost", "Host name")
	flag.IntVar(&port, "port", 5432, "TCP port")
	flag.StringVar(&name, "db", "", "Database name")
	flag.StringVar(&search, "search", "./sql/", "Search path for SQL files")
	flag.BoolVar(&dry, "dry", false, "Dry run mode")
	flag.BoolVar(&verbose, "verbose-sql", false, "Print out SQL")

	flag.Parse()

	search = path.Clean(search)

	db, err := sql.Open("pgx", fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", user, pass, host, port, name))
	if err != nil {
		fmt.Println(err)
		return
	}
	defer db.Close()

	options := &schema.Options{
		Dry:        dry,
		Verbose:    verbose,
		DB:         db,
		SearchPath: search,
	}

	msg := "Schema up to date"
	verb := "executed"
	if dry {
		msg = "Schema dry run complete"
		verb = "new"
	}

	stats, err := schema.Run(options)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
		return
	}

	took := time.Since(start)
	took = truncate_duration(took)

	fmt.Fprintf(os.Stderr, "%s (%d files, %d %s) in %s\n", msg, stats.Files, stats.New, verb, took)
}

func truncate_duration(d time.Duration) time.Duration {
	if d > time.Millisecond {
		d = d / time.Millisecond * time.Millisecond
	}
	if d > time.Millisecond*100 {
		d = d / (time.Millisecond * 100) * time.Millisecond * 100
	}
	if d > time.Second*10 {
		d = d / time.Second * time.Second
	}
	if d > time.Minute*10 {
		d = d / time.Minute * time.Minute
	}
	return d
}
