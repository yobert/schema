package main

import (
	"database/sql"
	"fmt"
	_ "github.com/jackc/pgx/stdlib"
	"github.com/yobert/schema"

	"flag"
	"path"
)

func main() {

	var (
		user   string
		pass   string
		host   string
		port   int
		name   string
		search string
	)

	flag.StringVar(&user, "user", "", "User")
	flag.StringVar(&pass, "pass", "", "Password")
	flag.StringVar(&host, "host", "localhost", "Host name")
	flag.IntVar(&port, "port", 5432, "TCP port")
	flag.StringVar(&name, "db", "", "Database name")
	flag.StringVar(&search, "search", "./sql/", "Search path for SQL files")

	flag.Parse()

	search = path.Clean(search)

	db, err := sql.Open("pgx", fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", user, pass, host, port, name))
	if err != nil {
		fmt.Println(err)
		return
	}
	defer db.Close()

	err = schema.Run(db, search)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("Schema up to date")
}
