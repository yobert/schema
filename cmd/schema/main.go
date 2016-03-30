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

	flag.StringVar(&user, "user", "", "User")
	flag.StringVar(&pass, "pass", "", "Password")
	flag.StringVar(&host, "host", "localhost", "Host name")
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
	if dry {
		msg = "Schema dry run complete"
	}

	stats, err := schema.Run(options)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
		return
	}

	fmt.Fprintf(os.Stderr, "%s (%d change files, %d new, %.2fs)\n", msg, stats.Files, stats.New, time.Since(start).Seconds())
}
