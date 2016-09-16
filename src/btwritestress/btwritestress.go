package main

import (
	"btutil"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"sync/atomic"

	"cloud.google.com/go/bigtable"
	"golang.org/x/net/context"
)

var (
	totalTimeMicros, numWrites uint64
)

func main() {
	var (
		project   = flag.String("project", "", "The name of the project.")
		instance  = flag.String("instance", "", "The name of the Cloud Bigtable instance.")
		authfile  = flag.String("authjson", "", "Google application credentials json file.")
		table     = flag.String("table", "", "Table to write metrics.")
		dps       = flag.Int("dps", 100000, "Data points per second.")
		numSavers = flag.Int("num_savers", 100, "num saving goroutines")
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

	ch1 := make(chan btutil.KeyValueEpochsec, *dps*10)

	go genMetrics(*dps, ch1)

	ch2 := make(chan []btutil.KeyValueEpochsec) //unbuffered channel
	go periodicallyDrainAndWriteToCh(ch1, 10000, time.Second, ch2)

	ctx := context.Background()
	log.Printf("num savers: [%v]", *numSavers)
	for i := 0; i < *numSavers; i++ {
		go saver(ctx, ch2, tbl)
	}

	go periodicallyPrintMetrics(ch1, *dps)

	select {}
}

func periodicallyPrintMetrics(ch <-chan btutil.KeyValueEpochsec, dps int) {
	for {
		n := atomic.LoadUint64(&numWrites)
		if n != 0 {
			avg := atomic.LoadUint64(&totalTimeMicros) / n
			log.Printf("dps: [%v], avg write time: [%v] micros, ch len: %v, cap: %v", dps, avg, len(ch), cap(ch))
		} else {
			log.Printf("no writes yet")
		}

		time.Sleep(time.Second * 5)
	}
}

func periodicallyDrainAndWriteToCh(input <-chan btutil.KeyValueEpochsec, maxSize int, timeout time.Duration,
	output chan<- []btutil.KeyValueEpochsec) {

	for {
		slice := drain(input, maxSize, timeout)
		output <- slice
	}
}

func drain(input <-chan btutil.KeyValueEpochsec, maxSize int, timeout time.Duration) []btutil.KeyValueEpochsec {
	timeoutCh := time.After(timeout)

	var slice []btutil.KeyValueEpochsec

	for {
		select {
		case kves := <-input:
			slice = append(slice, kves)
			if len(slice) >= maxSize {
				return slice
			}
		case <-timeoutCh:
			return slice
		}
	}
}

func saver(ctx context.Context, ch <-chan []btutil.KeyValueEpochsec, tbl *bigtable.Table) {
	for slice := range ch {
		if len(slice) != 0 {
			save(ctx, slice, tbl)
		}
	}
}

func save(ctx context.Context, slice []btutil.KeyValueEpochsec, tbl *bigtable.Table) {

	var rowKeys []string
	var muts []*bigtable.Mutation
	for _, e := range slice {
		mut := bigtable.NewMutation()
		mut.Set("0", "0", 0, e.ValueByteArray())

		muts = append(muts, mut)
		rowKeys = append(rowKeys, e.BTRowKeyStr())
	}

	start := time.Now()
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

	atomic.AddUint64(&totalTimeMicros, uint64(time.Since(start).Nanoseconds()/1000))
	atomic.AddUint64(&numWrites, uint64(len(slice)))
}

func genMetrics(n int, ch chan<- btutil.KeyValueEpochsec) {

	for {
		start := time.Now()
		for i := 0; i < n; i++ {
			kves := btutil.KeyValueEpochsec{getKey(i), float64(start.Unix()), uint32(start.Unix())}

			select {
			case ch <- kves:
			default:
				log.Fatalf("cannot write to ch. pctFull [%v]", pctFull(ch))
			}
		}

		timeTaken := time.Since(start)

		sleepDurationInNanos := 1000 * 1000 * 1000 - timeTaken.Nanoseconds()
		if sleepDurationInNanos < 0 {
			log.Printf("error - it takes more than 1 sec to generate [%v] metrics", n)
		}
		time.Sleep(time.Nanosecond * time.Duration(sleepDurationInNanos))
	}
}

func getKey(i int) string {
	return fmt.Sprintf("key_%v", i)
}

func pctFull(ch chan<- btutil.KeyValueEpochsec) float64 {
	return float64(len(ch)) * 100 / float64(cap(ch))
}
