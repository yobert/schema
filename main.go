package schema

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

var RunListMatch = regexp.MustCompile(`\D(\d{10})\D`)
var TableFromPathMatch = regexp.MustCompile(`/([^/]+)/[^/]+$`)

type RunList []string

func (l RunList) Len() int {
	return len(l)
}
func (l RunList) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}
func (l RunList) Less(i, j int) bool {
	m1 := RunListMatch.FindStringSubmatch(l[i])
	m2 := RunListMatch.FindStringSubmatch(l[j])
	if m1 == nil && m2 == nil {
		return l[i] < l[j]
	}
	if m1 == nil {
		return true
	}
	if m2 == nil {
		return false
	}
	i1, _ := strconv.Atoi(m1[1])
	i2, _ := strconv.Atoi(m2[1])
	if i1 < i2 {
		return true
	}
	return false
}

func Run(db *sql.DB, search_path string) error {
	err := CreateSchemaSupport(db)
	if err != nil {
		return err
	}

	list, err := LoadExisting(db)
	if err != nil {
		return err
	}

	unran, err := SearchUnran(db, list, search_path)
	if err != nil {
		return err
	}

	return Execute(db, unran)
}

func CreateSchemaSupport(db *sql.DB) error {

	has := 0
	row := db.QueryRow(`select count(1) as has from pg_namespace where nspname = $1 limit 1;`, "schemasupport")
	err := row.Scan(&has)
	if err != nil {
		return err
	}

	if has == 0 {
		_, err = db.Exec(`create schema schemasupport;`)
		if err != nil {
			return err
		}
	}

	has = 0
	row = db.QueryRow(`select count(1) as has from pg_tables where schemaname = $1 and tablename = $2 limit 1;`, "schemasupport", "files")
	err = row.Scan(&has)
	if err != nil {
		return err
	}

	if has == 0 {
		_, err = db.Exec(`create table schemasupport.files (name text not null, created timestamptz not null default now());`)
		return err
	}

	return nil
}

func LoadExisting(db *sql.DB) (map[string]bool, error) {
	ran := make(map[string]bool)
	rows, err := db.Query(`select name from schemasupport.files;`)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var name string

		if err := rows.Scan(&name); err != nil {
			return nil, err
		}

		ran[name] = true
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}

	return ran, nil
}

func SearchUnran(db *sql.DB, ran map[string]bool, search_path string) (RunList, error) {

	var files []string
	search := []string{"**/*.sql", "**/*.csv"}
	search_path_debug := ""

	for _, s := range search {
		p := search_path + "/" + s
		search_path_debug += " " + p
		fl, err := filepath.Glob(p)
		if err != nil {
			return nil, err
		}
		for _, f := range fl {
			files = append(files, f)
		}
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("No schema change files found (globbed%s)", search_path_debug)
	}

	run := make(RunList, 0)

	for _, f := range files {
		if !ran[f] {
			run = append(run, f)
		}
	}

	sort.Sort(run)

	return run, nil
}

func Execute(db *sql.DB, run RunList) error {

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	commit := false

	defer func() {
		if !commit {
			fmt.Println("Rolling back...")
			tx.Rollback()
		}
	}()

	for _, f := range run {
		fmt.Printf("Executing %s\n", f)

		if strings.HasSuffix(f, ".csv") {
			err := schema_run_csv(tx, f)
			if err != nil {
				return err
			}
		} else {
			err := schema_run_sql(tx, f)
			if err != nil {
				return err
			}
		}

		_, err := tx.Exec("insert into schemasupport.files (name) values ($1);", f)
		if err != nil {
			return err
		}
	}

	tx.Commit()
	commit = true
	return nil
}

func schema_run_sql(tx *sql.Tx, file string) error {
	raw, err := ioutil.ReadFile(file)
	if err != nil {
		return err
	}

	s := strings.Trim(string(raw), "\t\v\r\n ")
	_, err = tx.Exec(s)
	return err
}

func schema_run_csv(tx *sql.Tx, file string) error {
	// for now, guess the table name for inserting from the path to the changefile
	m := TableFromPathMatch.FindStringSubmatch(file)
	if m == nil || len(m) != 2 {
		return fmt.Errorf("Unable to figure out table name from changefile %#v", file)
	}

	table := m[1]

	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()

	var vals []interface{}
	isql := ""

	r := csv.NewReader(f)
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if isql == "" {
			if len(row) == 0 {
				return fmt.Errorf("No columns found in first line of file %#v", file)
			}
			vals = make([]interface{}, len(row))
			isql = "insert into " + table + " (" + strings.Join(row, ", ") + ") values ("
			for i, _ := range row {
				if i > 0 {
					isql += ", "
				}
				isql += "?"
			}
			isql += ");"
			continue
		}

		for i, v := range row {
			if v == "" {
				vals[i] = nil
			} else {
				vals[i] = v
			}
		}
		_, err = tx.Exec(isql, vals...)
		if err != nil {
			return err
		}
	}

	return nil
}
