package main

import (
	"encoding/csv"
	"log"
	"os"
)

func writeCsv(filename string, entries [][]string) {

	f, err := os.Create(filename)

	if err != nil {
		log.Fatalln("failed to open file", err)
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	for _, entry := range entries {
		if err := w.Write(entry); err != nil {
			log.Fatalln("error writing entry to file", err)
		}
	}
}
