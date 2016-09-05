package main

import (
	"btutil"
	"flag"
	"os"
)

func main() {

	var (
		project  = flag.String("project", "", "The name of the project.")
		instance = flag.String("instance", "", "The name of the Cloud Bigtable instance.")
		authfile = flag.String("authjson", "", "Google application credentials json file.")
		table    = flag.String("table", "", "The name of the table.")
	)

	flag.Parse()
	if *project == "" || *instance == "" || *authfile == "" || *table == "" {
		flag.Usage()
		os.Exit(1)
	}

	_, adminClient := btutil.Clients(*project, *instance, *authfile)

	btutil.CreateTableWithCF0IfMissing(adminClient, *table)
}
