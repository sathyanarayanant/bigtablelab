package main

import (
	"btutil"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"cloud.google.com/go/bigtable"
	"golang.org/x/net/context"
	"sync/atomic"
)

var (
	totalTimeMillis, numWrites uint64
)

func main() {
	var (
		project  = flag.String("project", "", "The name of the project.")
		instance = flag.String("instance", "", "The name of the Cloud Bigtable instance.")
		authfile = flag.String("authjson", "", "Google application credentials json file.")
		table    = flag.String("table", "", "Table to write metrics.")
		dps      = flag.String("dps", "", "Data points per second.")
		numSavers = flag.Int("num_savers", 100, "num saving goroutines")
	)
	//ex: bin/btwritestress -authjson ~/zdatalab-credentials.json -instance sathyatest -project zdatalab-1316 -table sec -dps 10000

	flag.Parse()
	if *project == "" || *instance == "" || *authfile == "" || *table == "" || *dps == "" {
		flag.Usage()
		os.Exit(1)
	}

	dataPointsPerSec, err := strconv.Atoi(*dps)
	if err != nil {
		log.Fatalf("cannot convert dps [%v] to int", *dps)
	}
	if dataPointsPerSec < 1 {
		log.Fatalf("invalid dps [%v], should be positive", *dps)
	}

	client, _ := btutil.Clients(*project, *instance, *authfile)
	tbl := client.Open(*table)

	const ch_buffer_size = 10000
	ch := make(chan []btutil.KeyValueEpochsec, ch_buffer_size)

	sleepDuration := time.Second * 5
	go genMetrics(dataPointsPerSec*5, sleepDuration, ch)

	ctx := context.Background()

	log.Printf("num savers: [%v]", *numSavers)
	for i := 0; i < *numSavers; i++ {
		go readChAndSaveToBT(ctx, ch, tbl, i)
	}

	go periodicallyPrintMetrics(ch, dataPointsPerSec)

	select {}
}

func periodicallyPrintMetrics(ch chan []btutil.KeyValueEpochsec, dps int) {
	for {
		n := atomic.LoadUint64(&numWrites)
		if n != 0 {
			avg := atomic.LoadUint64(&totalTimeMillis) / n
			log.Printf("dps: [%v], avg write time: [%v] millis, ch len: %v, cap: %v", dps, avg, len(ch), cap(ch))
		} else {
			log.Printf("no writes yet")
		}

		time.Sleep(time.Second * 5)
	}
}

func readChAndSaveToBT(ctx context.Context, ch <-chan []btutil.KeyValueEpochsec, tbl *bigtable.Table, saver int) {
	for slice := range ch {

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
			continue
		}
		if errors != nil {
			var i int
			for _, e := range errors {
				if err != nil {
					log.Printf("applybulk failed for rowkey [%v], err [%v]", rowKeys[i], e)
				}
				i++
			}
			continue
		}

		atomic.AddUint64(&totalTimeMillis, uint64(time.Since(start).Nanoseconds()/1000/1000))
		atomic.AddUint64(&numWrites, 1)
	}
}

func genMetrics(n int, sleepDuration time.Duration, ch chan<- []btutil.KeyValueEpochsec) {

	for {
		now := time.Now().Unix()
		const size = 10000
		var slice []btutil.KeyValueEpochsec
		var sliceOfSlice [][]btutil.KeyValueEpochsec
		var j int
		for i := 0; i < n; i++ {
			kves := btutil.KeyValueEpochsec{getKey(i), float64(now), uint32(now)}
			if j < size {
				j++
				slice = append(slice, kves)
			} else {
				sliceOfSlice = append(sliceOfSlice, slice)
				slice = make([]btutil.KeyValueEpochsec, 0)
				j = 0
			}
		}

		if len(slice) > 0 {
			sliceOfSlice = append(sliceOfSlice, slice)
		}

		for _, slice = range sliceOfSlice {
			select {
			case ch <- slice:
			default:
				log.Fatalf("cannot write to ch. pctFull [%v]", pctFull(ch))
			}
		}

		time.Sleep(sleepDuration)
	}

}

func getKey(i int) string {
	return fmt.Sprintf("key_%v", i)
}

func pctFull(ch chan<- []btutil.KeyValueEpochsec) float64 {
	return float64(len(ch)) * 100 / float64(cap(ch))
}
