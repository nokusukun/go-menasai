package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	logger "log"
	"os"
	"time"

	gomenasai "gitlab.com/nokusukun/go-menasai/manager"
)

var (
	BenchmarkStats []string
	RGBFront       = "38"
	RGBBack        = "48"
	RGBReset       = "\033[0m"
	logLevel       int
	log            *logger.Logger
)

func init() {
	flag.IntVar(&logLevel, "l", 5, "Log level")
	flag.Parse()
	var trace io.Writer
	if logLevel == 0 {
		trace = ioutil.Discard
	} else {
		trace = os.Stdout
	}
	log = logger.New(trace, "", 0)
}

func col(r, g, b int, t string) string {
	return fmt.Sprintf("\033[%v;2;%v;%v;%vm", t, r, g, b)
}

func benchmark(prefix string, target func()) {
	start := time.Now()
	target()
	end := time.Now()
	stat := fmt.Sprintf("\033[31;1;1m[Benchmark]%-20v:\033[0m %v\n", prefix, end.Sub(start))
	BenchmarkStats = append(BenchmarkStats, stat)
}

func printBenchmarkStats() {
	fmt.Println(col(123, 123, 200, RGBFront), "Benchmark Stats", RGBReset)
	for _, i := range BenchmarkStats {
		fmt.Print("🕒", i)
	}
}

func main() {
	manager, err := gomenasai.Load("testDB")
	if err != nil {
		panic(err)
	}

	fmt.Println(manager.ChunkPaths)
	result := manager.Search("watch")

	for _, doc := range result.Documents {
		fmt.Println(doc.ExportI()["title"])
		//fmt.Println(doc.ExportI()["description"])
	}
	fmt.Println("Filtered")
	result.Filter(`contains(doc.slug, "nike")`)

	for _, doc := range result.Documents {
		fmt.Println(doc.ExportI()["title"])
		fmt.Println(doc.ExportI()["slug"])
	}
}

func main2() {
	// manager := chunk.ChunkManager{}
	manager, err := gomenasai.New(&gomenasai.GomenasaiConfig{
		Name:           "TestDB",
		Path:           "testDB",
		ChunkSizeLimit: 8000,
		IndexPaths:     []string{"$.title", "$.description"},
	})

	if err != nil {
		panic(err)
	}

	jresp := []DjaliListing{}
	rawData, _ := ioutil.ReadFile("testdatadjali.json")
	err = json.Unmarshal(rawData, &jresp)
	if err != nil {
		panic(err)
	}
	resultCount := len(jresp)
	iterations := 0
	ids := []string{}
	benchmark(fmt.Sprintf("Inserting %v items", resultCount), func() {
		for i := 0; i <= iterations; i++ {
			for _, post := range jresp {
				id, err := manager.Insert(post)
				if err != nil {
					log.Printf("Error %v", err)
				}
				ids = append(ids, id)
				//fmt.Println("Inserted:", id)
			}
		}
	})

	manager.FlushSE()

	benchmark("Retrieval", func() {
		for _, id := range ids {
			doc, err := manager.Get(id)
			if err != nil {
				log.Printf("Error %v", err)
			}
			djali := DjaliListing{}
			doc.Export(&djali)
			//fmt.Println(djali.ParentPeer)
		}
	})

	benchmark("Searching", func() {
		docs := manager.Search("watch").
			Filter(`contains(doc.slug, "rolex")`).
			Filter(`contains(doc.slug, "daytona")`).
			Sort(`x.price.amount < y.price.amount`).
			Transform(`[{
				"operation": "shift",
				"spec": {
				  "title": "title",
				  "owner": "parentPeer",
				  "price": "price.amount"
				}
			  }]`)

		log.Println("Search and sort result for 'rolex watch': ", len(docs.Documents))

		for _, doc := range docs.Documents {
			//djali := DjaliListing{}
			log.Println(doc.ExportI())
			//doc.Export(&djali)
			//log.Println(djali.Price, djali.Title)
		}
		// benchmark("Filtering", func() {
		// 	docs.Filter(`contains(doc.slug, "fruit")`).Filter(`contains(doc.slug, "skateboard")`)
		// 	fmt.Println("Filter result for 'fruit' & 'skateboard': ", len(docs.Documents))
		// })
		//
		// benchmark("Exporting to JSON", func() {
		// 	jsonData, _ := docs.ExportJSONArray()
		// 	fmt.Println(string(jsonData)[:100], "...")
		// })
	})
	manager.Close()

	benchmark("Loading new DB", func() {
		manager, err = gomenasai.Load("testDB")
		if err != nil {
			panic(err)
		}
	})

	benchmark("Searching", func() {
		docs := manager.Search("watch").
			Filter(`contains(doc.slug, "rolex")`).
			Filter(`contains(doc.slug, "daytona")`).
			Sort(`x.price.amount < y.price.amount`)

		log.Println("Search, limit start-10;count-500 and sort result for 'watch': ", len(docs.Documents))

		for _, doc := range docs.Documents {
			djali := DjaliListing{}
			doc.Export(&djali)
			log.Println(djali.Price, djali.Title)
		}
		// benchmark("Filtering", func() {
		// 	docs.Filter(`contains(doc.slug, "fruit")`).Filter(`contains(doc.slug, "skateboard")`)
		// 	fmt.Println("Filter result for 'fruit' & 'skateboard': ", len(docs.Documents))
		// })
		//
		// benchmark("Exporting to JSON", func() {
		// 	jsonData, _ := docs.ExportJSONArray()
		// 	fmt.Println(string(jsonData)[:100], "...")
		// })
	})

	manager.Close()

	// func(dir string) error {
	// 	d, err := os.Open(dir)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	defer d.Close()
	// 	names, err := d.Readdirnames(-1)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	for _, name := range names {
	// 		err = os.RemoveAll(filepath.Join(dir, name))
	// 		if err != nil {
	// 			return err
	// 		}
	// 	}
	// 	os.RemoveAll(dir)
	// 	return nil
	// }("testdb")

	printBenchmarkStats()
	fmt.Println(col(100, 230, 90, RGBFront), "Test Complete ✅", RGBReset)
}
