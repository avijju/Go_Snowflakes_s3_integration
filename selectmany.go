// Example: Fetch many rows and allow cancel the query by Ctrl+C.
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime/debug"
	"runtime/pprof"
	"strconv"

	sf "github.com/snowflakedb/gosnowflake"
)

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var memprofile = flag.String("memprofile", "", "write memory profile to this file")

// getDSN constructs a DSN based on the test connection parameters
func getDSN() (string, *sf.Config, error) {
	env := func(k string, failOnMissing bool) string {
		if value := os.Getenv(k); value != "" {
			return value
		}
		if failOnMissing {
			log.Fatalf("%v environment variable is not set.", k)
		}
		return ""
	}
	//the config for connecting the snowflakes sql
	account := "kw74704.ap-south-1.aws"
	user := "avinashar"
	password := "MIPLinfo@1234"
	host := env("https://kw74704.ap-south-1.aws.snowflakecomputing.com/", false)
	port := env("443", false)
	protocol := env("SNOWFLAKE_TEST_PROTOCOL", false)

	portStr, _ := strconv.Atoi(port)
	cfg := &sf.Config{
		Account:  account,
		User:     user,
		Password: password,
		Host:     host,
		Port:     portStr,
		Protocol: protocol,
		Role:     "ACCOUNTADMIN",
	}

	dsn, err := sf.DSN(cfg)
	return dsn, cfg, err
}

// run is an actual main
func run(dsn string) {
	// handler interrupt signal
	ctx, cancel := context.WithCancel(context.Background())
	c := make(chan os.Signal, 1)
	defer close(c)
	signal.Notify(c, os.Interrupt)
	defer func() {
		signal.Stop(c)
	}()
	go func() {
		select {
		case <-c:
			cancel()
		case <-ctx.Done():
		}
	}()
	//open connection to snoflakes via dsn created before from the config passed
	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		log.Fatalf("failed to connect. %v, err: %v", dsn, err)
	}
	defer db.Close()
	//query to be executed to transfer the data
	query := `copy into 's3://logtergolang/a.parquet'
	from GODATAPIPELINE.PUBLIC.s3_int_stream
	storage_integration = s3_int`
	fmt.Printf("Executing a query. It may take long. You may stop by Ctrl+C.\n")
	//query executing
	rows, errr := db.QueryContext(ctx, query)
	//convert the query to csv file using github.com/joho/sqltocsv for now this is hardcoded file name, we can make this name as dynamic
	//errr := sqltocsv.WriteFile("important_user_report2.csv", rows)
	if errr != nil {
		fmt.Printf(errr.Error())
	}
	//sleep till csv created
	defer rows.Close()
	//below part is test one to printin the result of the query
	var v1 sql.NullString
	var v2 sql.NullString
	var v3 sql.NullString

	fmt.Printf("Fetching the results. It may take long. You may stop by Ctrl+C.\n")
	counter := 0
	//sqltocsv.WriteFile("~/important_user_report.csv", rows)
	for rows.Next() {
		err := rows.Scan(&v1, &v2, &v3)
		if err != nil {
			log.Fatalf("failed to get result. err: %v", err)
		}
		//if counter%10000 == 0 {

		if err != nil {
			log.Fatal(err)
		}

		fmt.Println("done")
		fmt.Printf("data: %v, %v, %v", v1, v2, v3)
		//}
		if counter%1000000 == 0 {
			debug.FreeOSMemory()
		}
		counter++
	}
	if rows.Err() != nil {
		fmt.Printf("ERROR: %v\n", rows.Err())
		return
	}

	fmt.Printf("Congrats! You have successfully run %v with Snowflake DB!\n", query)
}

func main() {
	if !flag.Parsed() {
		flag.Parse()
	}

	dsn, cfg, err := getDSN()
	if err != nil {
		log.Fatalf("failed to create DSN from Config: %v, err: %v", cfg, err)
	}

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	run(dsn)

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.WriteHeapProfile(f)
		f.Close()
		return
	}
}
