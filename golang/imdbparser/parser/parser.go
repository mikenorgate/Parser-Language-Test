package parser

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
)

type Parser struct {
}

func (parser *Parser) Parse(reader io.ReadCloser) ([]Title, error) {
	defer reader.Close()
	bytes, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	data := string(bytes)
	data = strings.TrimSpace(data)
	lines := strings.Split(data, "\n")

	results := make([]Title, len(lines))
	total := 0
	for i, line := range lines[1:] {
		columns := strings.Split(line, "\t")
		if len(columns) != 9 {
			return nil, fmt.Errorf("error processing")
		}
		results[i] = Title{
			TConst:         columns[0],
			TitleType:      columns[1],
			PrimaryTitle:   columns[2],
			OriginalTitle:  columns[3],
			IsAdult:        columns[4],
			StartYear:      columns[5],
			EndYear:        columns[6],
			RuntimeMinutes: columns[7],
			Genres:         columns[8],
		}
		total++
	}

	return results[:total], nil
}

func (parser *Parser) ParseConcurrent(r io.ReadCloser) ([]Title, error) {
	batchSize := 100000
	numWorkers := 8
	defer r.Close()

	rowPool := sync.Pool{New: func() interface{} {
		return make([]string, batchSize)
	}}

	titlePool := sync.Pool{New: func() interface{} {
		return make([]Title, batchSize)
	}}

	reader := func(ctx context.Context) <-chan []string {
		out := make(chan []string, 100)

		scanner := bufio.NewScanner(r)

		go func() {
			defer close(out)

			rowsBatch := rowPool.Get().([]string)
			i := 0
			for {
				scanned := scanner.Scan()

				select {
				case <-ctx.Done():
					return
				default:
					row := scanner.Text()
					if i == len(rowsBatch) || !scanned {
						out <- rowsBatch[0:i]
						rowsBatch = rowPool.Get().([]string)
						i = 0
					}
					rowsBatch[i] = row
					i++
				}

				if !scanned {
					return
				}
			}
		}()

		return out
	}

	worker := func(ctx context.Context, rowBatch <-chan []string) <-chan []Title {
		out := make(chan []Title, 100)

		go func() {
			defer close(out)

			for rowBatch := range rowBatch {
				tiles := titlePool.Get().([]Title)
				for i, row := range rowBatch {
					columns := strings.Split(row, "\t")
					tiles[i] = Title{
						TConst:         columns[0],
						TitleType:      columns[1],
						PrimaryTitle:   columns[2],
						OriginalTitle:  columns[3],
						IsAdult:        columns[4],
						StartYear:      columns[5],
						EndYear:        columns[6],
						RuntimeMinutes: columns[7],
						Genres:         columns[8],
					}
				}
				out <- tiles[0:len(rowBatch)]
				rowPool.Put(rowBatch)
			}
		}()

		return out
	}

	combiner := func(ctx context.Context, inputs ...<-chan []Title) <-chan []Title {
		out := make(chan []Title, 100)

		var wg sync.WaitGroup
		multiplexer := func(p <-chan []Title) {
			defer wg.Done()

			for in := range p {
				select {
				case <-ctx.Done():
				case out <- in:
				}
			}
		}

		wg.Add(len(inputs))
		for _, in := range inputs {
			go multiplexer(in)
		}

		go func() {
			wg.Wait()
			close(out)
		}()

		return out
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rowCh := reader(ctx)

	workerCh := make([]<-chan []Title, numWorkers)
	for i := 0; i < numWorkers; i++ {
		workerCh[i] = worker(ctx, rowCh)
	}

	var result []Title

	for processed := range combiner(ctx, workerCh...) {
		result = append(result, processed...)
		titlePool.Put(processed)
	}

	return result, nil
}
