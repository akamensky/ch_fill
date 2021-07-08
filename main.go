package main

import (
	"database/sql"
	"fmt"
	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/akamensky/argparse"
	"math/rand"
	"os"
	"time"
)

/*
CREATE TABLE test (`tsUnix` Int64, `tsDateTime` DateTime64(3), `tsString` String, `rndNumber` UInt32) ENGINE = MergeTree PARTITION BY toStartOfHour(tsDateTime) ORDER BY tsDateTime
*/

func main() {
	rand.Seed(time.Now().UTC().UnixNano())

	p := argparse.NewParser("ch_fill", "Fill Clickhouse database with data")

	dsnArg := p.String("d", "dsn", &argparse.Options{
		Required: false,
		Help:     "Clickhouse DSN for native protocol communication",
		Default:  "tcp://localhost:9000/?database=default&username=default",
	})

	tableArg := p.String("t", "table", &argparse.Options{
		Required: false,
		Help:     "Table name",
		Default:  "test",
	})

	batchSizeArg := p.Int("b", "batch-size", &argparse.Options{
		Required: false,
		Help:     "Batch size for bulk inserts",
		Default:  10000,
	})

	maxRecordsArg := p.Int("r", "records", &argparse.Options{
		Required: false,
		Help:     "Number of records to insert in total",
		Default:  28800000,
	})

	err := p.Parse(os.Args)
	if err != nil {
		panic(err)
	}

	dsn := *dsnArg
	tableName := *tableArg
	batchSize := *batchSizeArg
	maxRecords := *maxRecordsArg
	s := fmt.Sprintf("INSERT INTO %s (tsUnix, tsDateTime, tsString, rndNumber) VALUES (?, ?, ?, ?)", tableName)

	c, err := sql.Open("clickhouse", dsn)
	if err != nil {
		panic(err)
	}

	for batch := 0; batch < maxRecords/batchSize; batch++ {
		tx, err := c.Begin()
		if err != nil {
			panic(err)
		}
		stmt, err := tx.Prepare(s)
		if err != nil {
			panic(err)
		}
		for recordInBatch := 0; recordInBatch < batchSize; recordInBatch++ {
			millisSinceEpoch := recordInBatch + (batch * batchSize)
			ts := time.Unix(0, 0).Add(time.Duration(millisSinceEpoch) * time.Millisecond)
			rnd := rand.Int31n(1000)
			_, err = stmt.Exec(ts.UnixNano(), ts, fmt.Sprint(rnd), uint32(rnd))
			if err != nil {
				panic(err)
			}
		}
		err = tx.Commit()
		if err != nil {
			panic(err)
		}
		err = stmt.Close()
		if err != nil {
			panic(err)
		}
		fmt.Println("Inserted batch", batch+1)
	}
}
