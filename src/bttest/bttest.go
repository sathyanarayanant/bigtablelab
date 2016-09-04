package main

import (
	"os"
	"golang.org/x/net/context"
	"cloud.google.com/go/bigtable"
	"log"
	"io/ioutil"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

func main() {

	const file = "/Users/sathya/Downloads/zdatalab-202b6b4721f7.json"
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", file)

	jsonKey, err := ioutil.ReadFile(file)
	if err != nil {
		log.Fatalf("cannot read file [%v]", file)
	}

	config, err := google.JWTConfigFromJSON(jsonKey, bigtable.Scope)

	ctx := context.Background()
	client, err := bigtable.NewClient(ctx, "zdatalab-1316", "sathyatest", option.WithTokenSource(config.TokenSource(ctx)))
	if err !=  nil {
		log.Fatalf("cannot create bigtable client, err [%v]", err)
	}

	log.Printf("creating admin client")
	adminClient, err := bigtable.NewAdminClient(ctx, "zdatalab-1316", "sathyatest", option.WithTokenSource(config.TokenSource(ctx)))
	if err !=  nil {
		log.Fatalf("cannot create admin client, err [%v]", err)
	}
	log.Printf("created admin client [%v]", adminClient)

	log.Printf("creating table table1")
	err = adminClient.CreateTable(ctx, "table1")
	if err !=  nil {
		log.Fatalf("cannot create table1, err [%v]", err)
	}

	tbl := client.Open("table1")

	r, err := tbl.ReadRow(ctx, "com.google.cloud")
	if err != nil {
		log.Fatalf("err [%v]", err)
	}
	log.Printf("r = [%v]", r)
}
