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
	"fmt"
	"google.golang.org/grpc/credentials"
)

type queryCondition struct {
	target string
	from, until time.Time
}

func main() {
	var (
		project   = flag.String("project", "", "The name of the project.")
		instance  = flag.String("instance", "", "The name of the Cloud Bigtable instance.")
		authfile = flag.String("authjson", "", "Google application credentials json file.")
		qps = flag.Int("qps", 1000, "queries per second. ")
	)

	//eg: bin/btreadstress  -authjson ~/zdatalab-credentials.json -instance sathyatest -project zdatalab-1316 -qps 500

	flag.Parse()
	if *project == "" || *instance == "" || *authfile == "" {
		flag.Usage()
		os.Exit(1)
	}

	client, _ := btutil.Clients(*project, *instance, *authfile)
	tbl := client.Open("sec")

	ch := make(chan queryCondition, *qps * 5)

	go genQueries(*qps, ch)

	ctx := context.Background()
	const num_query_workers = 100
	for i := 0; i < num_query_workers; i++ {
		go queryWorker(ctx, ch, tbl)
	}

	select {}
}

func queryWorker(ctx context.Context, ch <-chan queryCondition, tbl *bigtable.Table) {
	for qc := range ch {
		query(ctx, qc, tbl)
	}
}

func query(ctx context.Context, qc queryCondition, tbl *bigtable.Table) {
	start := time.Now()
	begin := btutil.KeyValueEpochsec{qc.target, 0, uint32(qc.from.Unix())}
	end := btutil.KeyValueEpochsec{qc.target, 0, uint32(qc.until.Unix())}
	rr := bigtable.NewRange(begin.BTRowKeyStr(), end.BTRowKeyStr())

	type TimeValue struct {
		Epochsec uint32
		Value    float64
	}
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

	log.Printf("query completed in [%v]", time.Since(start))
}

func genQueries(n int, ch chan<- queryCondition) {

	for {
		for i := 0; i < n; i++ {
			qc := queryCondition{target: getKey(i), from: time.Now().Add(-time.Minute * 5), until: time.Now()}

			select {
			case ch <- qc:
			default:
				log.Fatalf("cannot write to ch. pctFull [%v]", pctFull(ch))
			}
		}

		log.Printf("ch pctFull [%v]", pctFull(ch))
		time.Sleep(time.Second * 1)
	}
}

func getKey(i int) string {
	return fmt.Sprintf("key_%v", i)
}

func pctFull(ch chan<- queryCondition) float64 {
	return float64(len(ch)) / float64(cap(ch))
}
