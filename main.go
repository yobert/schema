package schema

import (
	"crypto/md5"
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

var (
	FileListMatch      = regexp.MustCompile(`\D(\d{10})\D`)
	TableFromPathMatch = regexp.MustCompile(`/([^/]+)/[^/]+$`)
	PlaceholderMatch   = regexp.MustCompile(`\$\d+`)
)

type Stats struct {
	Files int
	New   int
}

type Options struct {
	DB         *sql.DB
	SearchPath string
	Dry        bool
	Verbose    bool

	would_have_made_files_table bool
}

type File struct {
	Path string
	MD5  string
}

type List []File

func (l List) Len() int {
	return len(l)
}
func (l List) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}
func (l List) Less(i, j int) bool {
	m1 := FileListMatch.FindStringSubmatch(l[i].Path)
	m2 := FileListMatch.FindStringSubmatch(l[j].Path)
	if m1 == nil && m2 == nil {
		return l[i].Path < l[j].Path
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

func Run(options *Options) (*Stats, error) {

	stats := &Stats{}

	err := CreateSchemaSupport(options)
	if err != nil {
		return stats, err
	}

	old_list, err := LoadExisting(options)
	if err != nil {
		return stats, err
	}

	new_list, err := Search(options)
	if err != nil {
		return stats, err
	}

	if len(new_list) == 0 {
		return stats, fmt.Errorf("No schema change files found in %#v", options.SearchPath)
	}

	unran, err := Merge(options, old_list, new_list, stats)
	if err != nil {
		return stats, err
	}

	return stats, Execute(options, unran, stats)
}

func CreateSchemaSupport(options *Options) error {
	db := options.DB

	has := 0
	row := db.QueryRow(`select count(1) as has from pg_namespace where nspname = $1 limit 1;`, "schemasupport")
	err := row.Scan(&has)
	if err != nil {
		return err
	}

	if has == 0 {
		sql := `create schema schemasupport;`
		if options.Verbose {
			fmt.Println(sql)
			fmt.Println()
		}
		if !options.Dry {
			_, err = db.Exec(sql)
			if err != nil {
				return err
			}
		}
	}

	has = 0
	row = db.QueryRow(`select count(1) as has from pg_tables where schemaname = $1 and tablename = $2 limit 1;`, "schemasupport", "files")
	err = row.Scan(&has)
	if err != nil {
		return err
	}

	if has == 0 {
		sql := `create table schemasupport.files (path text not null, created timestamptz not null default now(), md5 text not null);`
		if options.Verbose {
			fmt.Println(sql)
			fmt.Println()
		}
		if options.Dry {
			options.would_have_made_files_table = true
		} else {
			_, err = db.Exec(sql)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func LoadExisting(options *Options) (List, error) {
	db := options.DB

	l := make(List, 0)

	// Don't error out on the files table being missing if we're in dry run
	// mode. The table would be created and be empty anyhow.
	if options.Dry && options.would_have_made_files_table {
		return l, nil
	}

	rows, err := db.Query(`select path, md5 from schemasupport.files;`)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var f File

		if err := rows.Scan(&f.Path, &f.MD5); err != nil {
			return nil, err
		}

		l = append(l, f)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}

	return l, nil
}

func Search(options *Options) (List, error) {
	var files List

	search := []string{"**/*.sql", "**/*.csv"}

	for _, s := range search {
		p := options.SearchPath + "/" + s
		fl, err := filepath.Glob(p)
		if err != nil {
			return nil, err
		}
		for _, fpath := range fl {
			h, err := file_md5(fpath)
			if err != nil {
				return nil, err
			}

			files = append(files, File{
				Path: fpath,
				MD5:  h,
			})
		}
	}

	sort.Sort(files)

	return files, nil
}

func Merge(options *Options, old_list List, new_list List, stats *Stats) (List, error) {

	paths := make(map[string]File)
	md5s := make(map[string]File)

	for _, f := range old_list {
		paths[f.Path] = f
		md5s[f.MD5] = f
	}

	run := make(List, 0)

	for _, f := range new_list {
		stats.Files++

		p, ok := paths[f.Path]
		if ok {
			if p.MD5 == f.MD5 {
				// good, already ran, hash still matches
				continue
			}
			// TODO I guess allow a workaround?
			return nil, fmt.Errorf("Change file %#v has been modified: md5 %#v expected %#v",
				f.Path, f.MD5, p.MD5)
		}

		p, ok = md5s[f.MD5]
		if ok {
			return nil, fmt.Errorf("Change file %#v has already been run from path %#v",
				f.Path, p.Path)
		}

		run = append(run, f)
	}

	return run, nil
}

func Execute(options *Options, run List, stats *Stats) error {
	db := options.DB

	if len(run) == 0 {
		// Nothing to do
		return nil
	}

	if options.Verbose {
		fmt.Println("begin;")
		fmt.Println()
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	commit := false

	defer func() {
		if !commit {
			if options.Verbose {
				fmt.Println(`rollback;`)
				fmt.Println()
			}
			err := tx.Rollback()
			if err != nil {
				fmt.Println("Transaction rollback error:", err)
			}
		}
	}()

	for _, f := range run {
		fmt.Printf("-- %s\n", f.Path)

		if strings.HasSuffix(f.Path, ".csv") {
			err := schema_run_csv(options, tx, f.Path)
			if err != nil {
				return err
			}
		} else {
			err := schema_run_sql(options, tx, f.Path)
			if err != nil {
				return err
			}
		}

		sql := `insert into schemasupport.files (path, md5) values ($1, $2);`
		if options.Verbose {
			fmt.Println(debug_substitute(sql, f.Path, f.MD5))
			fmt.Println()
		}
		if !options.Dry {
			_, err := tx.Exec(sql, f.Path, f.MD5)
			if err != nil {
				return err
			}
		}

		stats.New++
	}

	commit = true

	if options.Verbose {
		fmt.Println(`commit;`)
		fmt.Println()
	}
	if !options.Dry {
		err := tx.Commit()
		if err != nil {
			return err
		}
	}

	return nil
}

func file_md5(fpath string) (string, error) {
	fh, err := os.Open(fpath)
	if err != nil {
		return "", err
	}
	defer fh.Close()

	h := md5.New()
	if _, err := io.Copy(h, fh); err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func schema_run_sql(options *Options, tx *sql.Tx, file string) error {
	raw, err := ioutil.ReadFile(file)
	if err != nil {
		return err
	}

	s := strings.Trim(string(raw), "\t\v\r\n ")
	if options.Verbose {
		fmt.Println(s)
	}
	if !options.Dry {
		_, err = tx.Exec(s)
		if err != nil {
			return err
		}
	}
	return nil
}

func schema_run_csv(options *Options, tx *sql.Tx, file string) error {
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
				isql += fmt.Sprintf("$%d", i+1)
			}
			isql += ");"
			continue
		}

		for i, v := range row {
			vals[i] = v
		}

		if options.Verbose {
			fmt.Println(debug_substitute(isql, row...))
		}
		if !options.Dry {
			_, err = tx.Exec(isql, vals...)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func debug_substitute(sql string, args ...string) string {

	sql = PlaceholderMatch.ReplaceAllStringFunc(sql, func(in string) string {
		out := in[1:] // chop off $
		v, err := strconv.Atoi(out)
		if err != nil {
			return in
		}
		v--

		if v < 0 || v > len(args)-1 {
			return in
		}

		return quote(args[v])
	})

	return sql
}

func quote(in string) string {
	out := ""
	crappy := false

	for _, v := range in {
		if (v >= 'a' && v <= 'z') ||
			(v >= 'A' && v <= 'Z') ||
			(v >= '0' && v <= '9') ||
			v == ' ' || v == '/' || v == '.' || v == '_' {
			out += string(v)
		} else {
			crappy = true
			if v < 128 {
				out += fmt.Sprintf("\\x%02X", v)
			} else if v < 65535 {
				out += fmt.Sprintf("\\u%04X", v)
			} else {
				out += fmt.Sprintf("\\U%08X", v)
			}
		}
	}
	if crappy {
		return "E'" + out + "'"
	}
	return "'" + out + "'"
}
