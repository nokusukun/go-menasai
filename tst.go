package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"time"

	gomenasai "gitlab.com/nokusukun/go-menasai/manager"
)

var (
	SearchQ string
)

func init() {
	flag.StringVar(&SearchQ, "q", "", "Query")
	flag.Parse()
}

func benchmark(prefix string, target func()) {
	start := time.Now()
	target()
	end := time.Now()
	fmt.Printf("[Benchmark]%v: %v\n", prefix, end.Sub(start))
}

func main() {
	// manager := chunk.ChunkManager{}
	manager, err := gomenasai.New(&gomenasai.GomenasaiConfig{
		Name:           "TestDB",
		Path:           "testDB",
		ChunkSizeLimit: 1024,
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
	benchmark(fmt.Sprintf("Inserting %vx%v items", resultCount, iterations+1), func() {
		for i := 0; i <= iterations; i++ {
			for _, post := range jresp {
				id, err := manager.Insert(post)
				if err != nil {
					fmt.Printf("Error %v", err)
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
				fmt.Printf("Error %v", err)
			}
			djali := DjaliListing{}
			doc.Export(&djali)
			//fmt.Println(djali.ParentPeer)
		}
	})

	benchmark("Searching", func() {
		docs := manager.Search("rolex watch").
			Filter(`contains(doc.slug, "rolex")`).
			Filter(`contains(doc.slug, "daytona")`).
			Sort(`x.price.amount < y.price.amount`)

		fmt.Println("Search and sort result for 'rolex watch': ", len(docs.Documents))

		for _, doc := range docs.Documents {
			djali := DjaliListing{}
			doc.Export(&djali)
			fmt.Println(djali.Price, djali.Title)
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
		docs := manager.Search("rolex watch").
			Filter(`contains(doc.slug, "rolex")`).
			Filter(`contains(doc.slug, "daytona")`).
			Sort(`x.price.amount < y.price.amount`)

		fmt.Println("Search and sort result for 'rolex daytona': ", len(docs.Documents))

		for _, doc := range docs.Documents {
			djali := DjaliListing{}
			doc.Export(&djali)
			fmt.Println(djali.Price, djali.Title)
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
}
