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
)

func main() {
	var (
		project  = flag.String("project", "", "The name of the project.")
		instance = flag.String("instance", "", "The name of the Cloud Bigtable instance.")
		authfile = flag.String("authjson", "", "Google application credentials json file.")
		table    = flag.String("table", "", "Table to write metrics.")
		dps      = flag.String("dps", "", "Data points per second.")
	)

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
	tbl := client.Open("sec")

	const ch_buffer_size = 10 * 1000 * 1000
	ch := make(chan btutil.KeyValueEpochsec, ch_buffer_size)

	sleepDuration := time.Second * 5
	go genMetrics(dataPointsPerSec*5, sleepDuration, ch)

	ctx := context.Background()
	//read from channel and write to bigtable
	for e := range ch {
		mut := bigtable.NewMutation()
		mut.Set("0", "0", 0, e.ValueByteArray())

		err := tbl.Apply(ctx, string(e.BTRowKey()), mut)
		if err != nil {
			log.Printf("got error [%v] when applying mutation", err)
		}
	}

	select {}
}

func genMetrics(n int, sleepDuration time.Duration, ch chan<- btutil.KeyValueEpochsec) {

	for {
		now := time.Now().Unix()
		for i := 0; i < n; i++ {
			kves := btutil.KeyValueEpochsec{getKey(i), float64(now), uint32(now)}

			select {
			case ch <- kves:
			default:
				log.Fatalf("cannot write to ch. pctFull [%v]", pctFull(ch))
			}
		}

		log.Printf("ch pctFull [%v]", pctFull(ch))
		time.Sleep(sleepDuration)
	}

}

func getKey(i int) string {
	return fmt.Sprintf("key_%v", i)
}

func pctFull(ch chan<- btutil.KeyValueEpochsec) float64 {
	return float64(len(ch)) / float64(cap(ch))
}
