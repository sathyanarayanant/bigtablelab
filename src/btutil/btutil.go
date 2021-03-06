package btutil

import (
	"io/ioutil"
	"log"

	"crypto/md5"
	"errors"
	"fmt"

	"cloud.google.com/go/bigtable"
	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
	"strconv"
)

func Clients(project, instance, authfile string) (*bigtable.Client, *bigtable.AdminClient) {
	jsonKey, err := ioutil.ReadFile(authfile)
	if err != nil {
		log.Fatalf("cannot read file [%v]", authfile)
	}

	config, err := google.JWTConfigFromJSON(jsonKey, bigtable.Scope, bigtable.AdminScope)

	ctx := context.Background()

	log.Printf("creating admin client")
	adminClient, err := bigtable.NewAdminClient(ctx, project, instance, option.WithTokenSource(config.TokenSource(ctx)))
	if err != nil {
		log.Fatalf("cannot create admin client, err [%v]", err)
	}
	log.Printf("created admin client [%v]", adminClient)

	client, err := bigtable.NewClient(ctx, project, instance, option.WithTokenSource(config.TokenSource(ctx)))
	if err != nil {
		log.Fatalf("cannot create bigtable client, err [%v]", err)
	}

	return client, adminClient
}

func CreateTableWithCF0IfMissing(adminClient *bigtable.AdminClient, table string) {
	ctx := context.Background()

	tables, err := adminClient.Tables(ctx)
	if err != nil {
		log.Fatalf("cannot get list of tables, err [%v]", err)
	}
	log.Printf("list of tables: %v", tables)

	set := make(map[string]bool)
	for _, e := range tables {
		set[e] = true
	}

	_, found := set[table]
	if found {
		log.Printf("table [%v] already found", table)
		return
	}

	log.Printf("creating table [%v]", table)
	err = adminClient.CreateTable(ctx, table)

	if err != nil {
		log.Fatalf("cannot create table [%v], err [%v]", table, err)
	}
	const column_family = "0"
	log.Printf("creating column family [%v] in table [%v]", column_family, table)

	err = adminClient.CreateColumnFamily(ctx, table, column_family)
	if err != nil {
		log.Fatalf("cannot create column family [%v], err [%v]", column_family, err)
	}
}

func GetBTKey(key string, epochSec uint32) string {
	md5Sum := fmt.Sprintf("%x", md5.Sum([]byte(key)))

	hour := int(epochSec / 3600)

	sevenCharStr, _ := GetNCharStrLeadingZeros(strconv.Itoa(hour), 7)

	return md5Sum + "_" + sevenCharStr
}

func GetNCharStrLeadingZeros(s string, n int) (string, error) {
	if len(s) > n {
		msg := fmt.Sprintf("cannot get %v char str for [%v]", s, n)
		log.Printf(msg)
		return "", errors.New(msg)
	}

	if len(s) == n {
		return s, nil
	}

	numZeros := n - len(s)
	var zeros string
	for i := 0; i < numZeros; i++ {
		zeros += "0"
	}

	return zeros + s, nil
}
