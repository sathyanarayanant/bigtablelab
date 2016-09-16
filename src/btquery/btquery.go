package main

import (
	"flag"
	"os"
	"btutil"
	"cloud.google.com/go/bigtable"
	"time"
	"golang.org/x/net/context"
	"log"
	"bytes"
	"encoding/binary"
)

func main() {
	var (
		project   = flag.String("project", "", "The name of the project.")
		instance  = flag.String("instance", "", "The name of the Cloud Bigtable instance.")
		authfile = flag.String("authjson", "", "Google application credentials json file.")
		key = flag.String("key", "", "The key for which to query the data for last 5 min.")
	)

	flag.Parse()
	if *project == "" || *instance == "" || *authfile == "" || *key == "" {
		flag.Usage()
		os.Exit(1)
	}

	client, _ := btutil.Clients(*project, *instance, *authfile)
	tbl := client.Open("sec")

	for {
		start := time.Now()
		begin := btutil.KeyValueEpochsec{*key, 0, uint32(time.Now().Add(-5 * time.Minute).Unix())}
		end := btutil.KeyValueEpochsec{*key, 0, uint32(time.Now().Unix())}
		rr := bigtable.NewRange(begin.BTRowKeyStr(), end.BTRowKeyStr())
		log.Printf("row range: %v", rr)

		type TimeValue struct {
			Epochsec uint32
			Value    float64
		}

		ctx := context.Background()
		var results []TimeValue
		err := tbl.ReadRows(ctx, rr, func(r bigtable.Row) bool {
			epochsec, err := btutil.RowKey(r.Key()).Epochsec()
			if err != nil {
				return true
			}

			var value float64
			for _, v := range r {
				for _, e := range v {
					buf := bytes.NewReader(e.Value)
					err := binary.Read(buf, binary.BigEndian, &value)
					if err != nil {
						log.Printf("got err when converting from []byte to float64 - [%v]", err)
						return true
					}
				}
			}

			results = append(results, TimeValue{epochsec, value})
			return true
		})
		if err != nil {
			log.Fatalf("got err when calling readrows. err [%v]", err)
		}

		log.Printf("results: %v", results)
		log.Printf("results obtained in [%v]", time.Since(start))
		time.Sleep(time.Second * 5)
	}
}
