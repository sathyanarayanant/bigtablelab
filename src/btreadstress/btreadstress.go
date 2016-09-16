package main

import (
	"btutil"
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"cloud.google.com/go/bigtable"
	"golang.org/x/net/context"
	"sync/atomic"
)

type queryCondition struct {
	target      string
	from, until time.Time
}

var (
	totalQueryTimeMillis, numQueries, totalLastDatapointAgeSeconds uint64
)

func main() {
	var (
		project  = flag.String("project", "", "The name of the project.")
		instance = flag.String("instance", "", "The name of the Cloud Bigtable instance.")
		authfile = flag.String("authjson", "", "Google application credentials json file.")
		qps      = flag.Int("qps", 1000, "queries per second. ")
		numQueryWorkers = flag.Int("num_query_workers", 100, "queries per second. ")
	)

	//eg: bin/btreadstress  -authjson ~/zdatalab-credentials.json -instance sathyatest -project zdatalab-1316 -qps 500

	flag.Parse()
	if *project == "" || *instance == "" || *authfile == "" {
		flag.Usage()
		os.Exit(1)
	}

	log.Printf("num query workers: [%v]", *numQueryWorkers)

	client, _ := btutil.Clients(*project, *instance, *authfile)
	tbl := client.Open("sec")

	ch := make(chan queryCondition, *qps*5)

	go genQueries(*qps, ch)

	ctx := context.Background()
	for i := 0; i < *numQueryWorkers; i++ {
		go queryWorker(ctx, ch, tbl)
	}

	go periodicallyPrintMetrics(ch, *qps)

	select {}
}

func periodicallyPrintMetrics(ch chan queryCondition, qps int) {
	for {
		n := atomic.LoadUint64(&numQueries)
		if n != 0 {
			avgTime := atomic.LoadUint64(&totalQueryTimeMillis) / n
			avgDelaySeconds := atomic.LoadUint64(&totalLastDatapointAgeSeconds) / n
			log.Printf("qps: [%v], avg query time: %v millis, avg delay: %v seconds, ch len: %v, cap: %v",
				qps, avgTime, avgDelaySeconds, len(ch), cap(ch))
		} else {
			log.Printf("no queries yet")
		}

		time.Sleep(time.Second * 5)
	}
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
		log.Printf("got err when calling readrows. err [%v]", err)
		return
	}

	atomic.AddUint64(&totalQueryTimeMillis, uint64(time.Since(start).Nanoseconds()/1000/1000))
	atomic.AddUint64(&numQueries, 1)

	if len(results) > 0 {
		age := uint32(time.Now().Unix()) - results[len(results) - 1].Epochsec
		atomic.AddUint64(&totalLastDatapointAgeSeconds, uint64(age))
	} else {
		log.Printf("empty result for qc: %+v", qc)
	}
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

		time.Sleep(time.Second * 1)
	}
}

func getKey(i int) string {
	return fmt.Sprintf("key_%v", i)
}

func pctFull(ch chan<- queryCondition) float64 {
	return float64(len(ch)) * 100 / float64(cap(ch))
}
