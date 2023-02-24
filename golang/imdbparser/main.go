package main

import (
	"github.com/mikenorgate/imdbparser/parser"
	"io"
	"log"
	"net/http"
	"os"
	"time"
)
import gzip "github.com/klauspost/pgzip"

func main() {

	file, err := os.Open("title.basics.tsv.gz")
	if os.IsNotExist(err) {
		file, err = os.Create("title.basics.tsv.gz")
		if err != nil {
			panic(err)
		}
		defer file.Close()

		resp, err := http.Get("https://datasets.imdbws.com/title.basics.tsv.gz")
		if err != nil {
			panic(err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			panic("Unable to download file")
		}

		_, err = io.Copy(file, resp.Body)
		if err != nil {
			panic(err)
		}

	} else {
		defer file.Close()
	}

	decompressed, err := gzip.NewReader(file)
	if err != nil {
		panic(err)
	}
	defer decompressed.Close()

	parser := parser.Parser{}

	start := time.Now()
	titles, err := parser.Parse(decompressed)
	if err != nil {
		panic(err)
	}
	elapsed := time.Since(start)

	log.Printf("Found %d titles in %dms", len(titles), elapsed.Milliseconds())
}
