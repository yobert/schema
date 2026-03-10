package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/joho/godotenv"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	start := time.Now()
	ctx := context.Background()

	var (
		search  string
		dry     bool
		verbose bool
	)

	flag.StringVar(&search, "search", "./sql/", "Search path for SQL files")
	flag.BoolVar(&dry, "dry", false, "Dry run mode")
	flag.BoolVar(&verbose, "verbose-sql", false, "Print out SQL")
	flag.Parse()

	search = path.Clean(search)

	if _, err := os.Stat(".env"); err == nil {
		if err := godotenv.Load(".env"); err != nil {
			return err
		}
	}

	config, err := parseConfig()
	if err != nil {
		return err
	}

	if config.Database == "" {
		return fmt.Errorf("Database not specified. Configure via .env file with one of DATABASE_URL, SCHEMA_DATABASE_URL, SCHEMA_PGDATABASE")
	}

	db, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	options := &Options{
		Ctx:        ctx,
		Dry:        dry,
		Verbose:    verbose,
		DB:         db,
		SearchPath: search,
	}

	msg := "up to date"
	if dry {
		msg = "dry run complete"
	}

	stats, err := Run(options)
	if err != nil {
		return err
	}

	took := time.Since(start)
	took = truncate_duration(took)

	fmt.Printf("Schema %s (%d files", msg, stats.Files)
	fmt.Printf(", %d new", stats.New)
	fmt.Printf(") in %s\n", took)
	return nil
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

// This is a little goofy. That's because I want it to assume the right thing in the typical case of
// your schema being defined next to your application, and so a .env file will define how the application
// talks to the database-- as opposed to how the schema tool should talk to the database, which will need
// elevated permissions.
//
// Most applications will just define DATABASE_URL. This tool will assume user "postgres" with no password and
// the same host setting as DATABASE_URL. These assumptions can be overridden by:
// SCHEMA_PGDATABASE
// SCHEMA_PGUSER
// SCHEMA_PGPASSWORD
// SCHEMA_PGHOST
// SCHEMA_PGPORT
func parseConfig() (*pgx.ConnConfig, error) {
	appconfig, err := pgx.ParseConfig(os.Getenv("DATABASE_URL"))
	if err != nil {
		return nil, fmt.Errorf("Error parsing DATABASE_URL: %w", err)
	}

	config, err := pgx.ParseConfig(os.Getenv("SCHEMA_DATABASE_URL"))
	if err != nil {
		return nil, err
	}

	if config.Database == "" {
		config.Database = appconfig.Database
	}
	if config.Host == "" {
		config.Host = appconfig.Host
	}
	if config.Port == 0 {
		config.Port = appconfig.Port
	}
	if config.User == "" {
		config.User = "postgres"
	}
	// default to no password

	if v := os.Getenv("SCHEMA_PGDATABASE"); v != "" {
		config.Database = v
	}
	if v := os.Getenv("SCHEMA_PGUSER"); v != "" {
		config.User = v
	}
	if v := os.Getenv("SCHEMA_PGPASSWORD"); v != "" {
		config.Password = v
	}
	if v := os.Getenv("SCHEMA_PGHOST"); v != "" {
		config.Host = v
	}
	if v := os.Getenv("SCHEMA_PGPORT"); v != "" {
		i, err := strconv.ParseUint(v, 10, 16)
		if err != nil {
			return nil, fmt.Errorf("Error parsing port number from SCHEMA_PGPORT: %w", err)
		}
		config.Port = uint16(i)
	}

	return config, nil
}
