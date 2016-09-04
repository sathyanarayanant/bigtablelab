package main

import (
	"io/ioutil"
	"log"

	"cloud.google.com/go/bigtable"
	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
	"flag"
	"os"
)

func main() {

	var (
		project   = flag.String("project", "", "The name of the project.")
		instance  = flag.String("instance", "", "The name of the Cloud Bigtable instance.")
		authfile = flag.String("authjson", "", "Google application credentials json file.")
	)

	flag.Parse()
	if *project == "" || *instance == "" || *authfile == "" {
		flag.Usage()
		os.Exit(1)
	}

	log.Printf("google app credentials file: [%v]", *authfile)
	jsonKey, err := ioutil.ReadFile(*authfile)
	if err != nil {
		log.Fatalf("cannot read file [%v]", *authfile)
	}

	config, err := google.JWTConfigFromJSON(jsonKey, bigtable.Scope, bigtable.AdminScope)

	ctx := context.Background()
	client, err := bigtable.NewClient(ctx, *project, *instance, option.WithTokenSource(config.TokenSource(ctx)))
	if err != nil {
		log.Fatalf("cannot create bigtable client, err [%v]", err)
	}

	log.Printf("creating admin client")
	adminClient, err := bigtable.NewAdminClient(ctx, "zdatalab-1316", "sathyatest", option.WithTokenSource(config.TokenSource(ctx)))
	if err != nil {
		log.Fatalf("cannot create admin client, err [%v]", err)
	}
	log.Printf("created admin client [%v]", adminClient)

	tables, err := adminClient.Tables(ctx)
	if err != nil {
		log.Fatalf("cannot get list of tables, err [%v]", err)
	}
	log.Printf("list of tables: %v", tables)

	set := make(map[string]bool)
	for _, e := range tables {
		set[e] = true
	}

	const table = "table1"
	_, found := set[table]
	if !found {
		log.Printf("creating table table1")
		err = adminClient.CreateTable(ctx, "table1")

		if err != nil {
			log.Fatalf("cannot create table1, err [%v]", err)
		}
		log.Printf("table1 created, creating column family")

		err = adminClient.CreateColumnFamily(ctx, "table1", "cf1")
		if err != nil {
			log.Fatalf("cannot create cf1, err [%v]", err)
		}
	}

	tbl := client.Open("table1")
	mut := bigtable.NewMutation()
	mut.Set("cf1", "c1", 0, []byte("A"))
	mut.Set("cf1", "c2", 0, []byte("B"))
	err = tbl.Apply(ctx, "r1", mut)
	if err != nil {
		log.Fatalf("cannot apply mutation, err [%v]", err)
	}

	log.Printf("reading row r1")
	r, err := tbl.ReadRow(ctx, "r1")
	if err != nil {
		log.Fatalf("err [%v]", err)
	}
	log.Printf("r = [%v]", r)
}
