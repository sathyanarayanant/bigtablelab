package main

import (
	"btutil"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"bytes"
	"crypto/md5"
	"encoding/binary"
	"math/rand"

	"cloud.google.com/go/bigtable"
	"golang.org/x/net/context"
)

func main() {
	var (
		project          = flag.String("project", "", "The name of the project.")
		instance         = flag.String("instance", "", "The name of the Cloud Bigtable instance.")
		authfile         = flag.String("authjson", "", "Google application credentials json file.")
		table            = flag.String("table", "", "Table to write metrics.")
		datapointsPerRow = flag.Int("datapoints_per_row", 50, "datapoints per row")
		numWriters       = flag.Int("num_writers", 100, "num saving goroutines")
		writeBatchSize   = flag.Int("num_rows_per_write", 10, "rows per write")
	)
	//ex: bin/btwritestress -authjson ~/zdatalab-credentials.json -instance sathyatest -project zdatalab-1316 -table sec -dps 10000

	flag.Parse()
	if *project == "" || *instance == "" || *authfile == "" || *table == "" {
		flag.Usage()
		os.Exit(1)
	}

	client, _ := btutil.Clients(*project, *instance, *authfile)
	tbl := client.Open(*table)

	ch := make(chan []KeyTimevalues) //unbuffered

	go genMetrics(*writeBatchSize, *datapointsPerRow, ch)

	counter := btutil.NewCounter()

	ctx := context.Background()
	log.Printf("num savers: [%v], write batch size [%v], data points per row [%v]",
		*numWriters, *writeBatchSize, *datapointsPerRow)
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

func writer(ctx context.Context, ch <-chan []KeyTimevalues, tbl *bigtable.Table, counter *btutil.Counter) {
	for slice := range ch {
		if len(slice) != 0 {
			write(ctx, slice, tbl, counter)
		}
	}
}

type SecofhourValue struct {
	SecOfHour uint16
	Value     float64
}

func toBigEndianBytes(slice []SecofhourValue) []byte {
	var buffer bytes.Buffer
	err := binary.Write(&buffer, binary.BigEndian, slice)
	if err != nil {
		log.Fatalf("cannot convert slice %+v to byte array, err [%v]")
	}

	return buffer.Bytes()
}

func md5Str(input []byte) string {
	md5bytes := md5.Sum(input)

	return string(md5bytes[:])
}

func write(ctx context.Context, slice []KeyTimevalues, tbl *bigtable.Table, counter *btutil.Counter) {

	const column_family = "0"

	var rowKeys []string
	var muts []*bigtable.Mutation
	var numDatapoints int
	for _, e := range slice {
		var secofhourValues []SecofhourValue
		for _, e2 := range e.Timevalues {
			//todo: case when falls to next hour
			secofhourValues = append(secofhourValues, SecofhourValue{uint16(e2.Epochsec % 3600), e2.Value})
			numDatapoints++
		}

		mut := bigtable.NewMutation()

		bytes := toBigEndianBytes(secofhourValues)
		mut.Set(column_family, md5Str(bytes), 0, bytes)

		muts = append(muts, mut)
		rowKeys = append(rowKeys, btutil.GetBTKey(e.Key, e.Timevalues[0].Epochsec))
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

	counter.Mark(numDatapoints)
}

type TimeValue struct {
	Epochsec uint32
	Value    float64
}

type KeyTimevalues struct {
	Key        string
	Timevalues []TimeValue
}

func genMetrics(numKeys int, datapointsPerKey int, ch chan<- []KeyTimevalues) {

	for {
		nowEpochsec := int(time.Now().Unix())

		var slice []KeyTimevalues
		for i := 0; i < numKeys; i++ {
			ktv := KeyTimevalues{Key: getKey(rand.Intn(1000 * 1000))}

			for j := 0; j < datapointsPerKey; j++ {
				epochsec := nowEpochsec - datapointsPerKey + j + 1
				ktv.Timevalues = append(ktv.Timevalues, TimeValue{uint32(epochsec), float64(epochsec)})
			}
			slice = append(slice, ktv)
		}

		ch <- slice
	}
}

func getKey(i int) string {
	return fmt.Sprintf("key_%v", i)
}
