package parser

import (
	b "bytes"
	"io"
	"math"
)

type Parser struct {
}

func (parser *Parser) Parse(reader io.ReadCloser) ([]Title, error) {
	defer reader.Close()

	buf := new(b.Buffer)
	io.Copy(buf, reader)
	bytes := buf.Bytes()

	bytes = b.TrimRight(bytes, "\n ")
	lines := b.Split(bytes, []byte("\n"))
	results := make([]Title, len(lines))

	worker := func(start int, end int) <-chan bool {
		out := make(chan bool)

		go func() {
			defer close(out)
			for i, line := range lines[start:end] {
				columns := b.Split(line, []byte("\t")) //strings.Split(line, "\t")
				current := &results[start+i]
				current.TConst = string(columns[0])
				current.TitleType = string(columns[1])
				current.PrimaryTitle = string(columns[2])
				current.OriginalTitle = string(columns[3])
				current.IsAdult = string(columns[4])
				current.StartYear = string(columns[5])
				current.EndYear = string(columns[6])
				current.RuntimeMinutes = string(columns[7])
				current.Genres = string(columns[8])
			}

			out <- true
		}()

		return out
	}

	numWorkers := 8
	processChan := make([]<-chan bool, numWorkers)
	chunkSize := int(math.Ceil(float64(len(lines)) / float64(numWorkers)))
	for i := 0; i < numWorkers; i++ {
		start := (i * chunkSize) + 1
		end := int(math.Min(float64(start)+float64(chunkSize), float64(len(lines))))
		processChan[i] = worker(start, end)
	}

	for _, c := range processChan {
		<-c
	}

	return results, nil
}
