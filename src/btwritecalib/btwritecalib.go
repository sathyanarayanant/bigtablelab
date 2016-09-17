package main

import (
	"btutil"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"cloud.google.com/go/bigtable"
	"golang.org/x/net/context"
)

func main() {
	var (
		project        = flag.String("project", "", "The name of the project.")
		instance       = flag.String("instance", "", "The name of the Cloud Bigtable instance.")
		authfile       = flag.String("authjson", "", "Google application credentials json file.")
		table          = flag.String("table", "", "Table to write metrics.")
		numWriters     = flag.Int("num_writers", 100, "num saving goroutines")
		writeBatchSize = flag.Int("write_batch_size", 1000, "write batch size")
	)
	//ex: bin/btwritestress -authjson ~/zdatalab-credentials.json -instance sathyatest -project zdatalab-1316 -table sec -dps 10000

	//optimal value for numSavers = num bigtable nodes * 100 for

	flag.Parse()
	if *project == "" || *instance == "" || *authfile == "" || *table == "" {
		flag.Usage()
		os.Exit(1)
	}

	client, _ := btutil.Clients(*project, *instance, *authfile)
	tbl := client.Open(*table)

	ch := make(chan []btutil.KeyValueEpochsec) //unbuffered

	go genMetrics(*writeBatchSize, ch)

	counter := btutil.NewCounter()

	ctx := context.Background()
	log.Printf("num savers: [%v], write batch size [%v]", *numWriters, *writeBatchSize)
	for i := 0; i < *numWriters; i++ {
		go writer(ctx, ch, tbl, counter)
	}

	go periodicallyPrintMetrics(counter)

	select {}
}

func periodicallyPrintMetrics(counter *btutil.Counter) {
	for {
		time.Sleep(time.Second * 5)

		log.Printf("dps out: %0.2f", counter.RatePerSec())
	}
}

func writer(ctx context.Context, ch <-chan []btutil.KeyValueEpochsec, tbl *bigtable.Table, counter *btutil.Counter) {
	for slice := range ch {
		if len(slice) != 0 {
			write(ctx, slice, tbl, counter)
		}
	}
}

func write(ctx context.Context, slice []btutil.KeyValueEpochsec, tbl *bigtable.Table, counter *btutil.Counter) {

	var rowKeys []string
	var muts []*bigtable.Mutation
	for _, e := range slice {
		mut := bigtable.NewMutation()
		mut.Set("0", "0", 0, e.ValueByteArray())

		muts = append(muts, mut)
		rowKeys = append(rowKeys, e.BTRowKeyStr())
	}

	errors, err := tbl.ApplyBulk(ctx, rowKeys, muts)
	if err != nil {
		log.Printf("entire bulk mutation failed. err [%v]", err)
		return
	}
	if errors != nil {
		var i int
		for _, e := range errors {
			if err != nil {
				log.Printf("applybulk failed for rowkey [%v], err [%v]", rowKeys[i], e)
			}
			i++
		}
		return
	}

	counter.Mark(len(slice))
}

func genMetrics(n int, ch chan<- []btutil.KeyValueEpochsec) {

	for {
		start := time.Now()

		var slice []btutil.KeyValueEpochsec
		for i := 0; i < n; i++ {
			kves := btutil.KeyValueEpochsec{getKey(i), float64(start.Unix()), uint32(start.Unix())}
			slice = append(slice, kves)
		}

		ch <- slice
	}
}

func getKey(i int) string {
	return fmt.Sprintf("key_%v", i)
}
