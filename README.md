# schema

Go package for executing database change files in various formats. Create an sql/ folder like this:

    vim sql/yourtable/00_create_or_whatever_$(date +%s).sql

Have one folder per table (or group of tables) as you see fit. Increase the 00_ by one for each new
file in the folder, and that way they will always be ordered nicely. SQL files are executed in order
of the unix timestamp in the filename-- this way you can have changes on a branch, and after merging
you won't have merge conflicts, and things will in general always happen in the order you'd expect.

Installation
------------

    go install github.com/yobert/schema

Or to be fancy, put in your go.mod:

    tool (
        github.com/yobert/schema
    )

And then you can just say `go tool github.com/yobert/schema` and it will run.

Configuration
-------------
Create a .env file with:

	SCHEMA_DATABASE_URL=postgres://user@host:password/dbname

It will also inherit some fields from a DATABASE_URL if present.
