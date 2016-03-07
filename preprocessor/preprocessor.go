package main

import (
	"bufio"
	"io/ioutil"
	"log"
	"os"
)

// the netflix ratings data is formatted in a weird way, this fixes it.
func main() {
	files, err := ioutil.ReadDir("training_set")
	if err != nil {
		log.Fatal(err)
		panic(err)
	}

	fout, err := os.Create("netflix_merged")
	if err != nil {
		log.Fatal(err)
		panic(err)
	}
	defer fout.Close()

	for _, fileName := range files {
		file, err := os.Open("training_set/" + fileName.Name())
		if err != nil {
			log.Fatal(err)
			panic(err)
		}

		first := true
		movie := 0
		scanner := bufio.NewScanner(file)
		var mvNr string
		for scanner.Scan() {
			if first && movie == 0 {
				mvNr = scanner.Text()
				mvNr = mvNr[0 : len(mvNr)-1]
			} else {
				line := scanner.Text()
				fout.WriteString(mvNr + "," + line + "\n")
			}
			first = false
			movie = 1
		}

		if err := scanner.Err(); err != nil {
			log.Fatal(err)
			panic(err)
		}

		file.Close()
	}
}
