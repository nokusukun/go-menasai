package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"time"

	"gitlab.com/nokusukun/go-menasai/chunk"
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
	myChunk, err := chunk.CreateChunk(&chunk.Config{
		ID:         "mychunk",
		Path:       "myDatabase/chunks/001.chk",
		IndexDir:   "myDatabase/chunks/index",
		IndexPaths: []string{"$.description", "$.title"},
	})

	if err != nil {
		panic(err)
	}

	// resp, _ := http.Get("https://www.justice.gov/api/v1/blog_entries.json")
	// jresp := JSONResponse{}
	// defer resp.Body.Close()
	// rawData, _ := ioutil.ReadAll(resp.Body)
	// json.Unmarshal(rawData, &jresp)
	jresp := []DjaliListing{}
	rawData, _ := ioutil.ReadFile("testdatadjali.json")
	err = json.Unmarshal(rawData, &jresp)
	if err != nil {
		panic(err)
	}
	resultCount := len(jresp)
	iterations := 0
	benchmark(fmt.Sprintf("Inserting %vx%v items", resultCount, iterations+1), func() {
		for i := 0; i <= iterations; i++ {
			for _, post := range jresp {
				myChunk.InsertAsync(post, true)
			}
		}
	})

	// myChunk = nil

	// newChunk, err := chunk.LoadChunk("myDatabase/chunks/001.chk")
	// if err != nil {
	// 	panic(err)
	// }
	var commitWait chan error
	benchmark("commit", func() {
		fmt.Println("Commiting to cold storage.")
		commitWait = myChunk.CommitAsync()
	})
	<-commitWait
	myChunk.FlushSE()

	// benchmark("lookup", func() {
	// 	result := myChunk.SearchIndex(SearchQ)
	// 	var data chan *chunk.Document

	// 	for _, res := range result.Docs.(rtypes.ScoredDocs) {
	// 		code := res.ScoredID.DocId
	// 		//fmt.Println("Result: ", code)
	// 		data = myChunk.GetAsync(code)
	// 		n := <-data
	// 		exported := DjaliListing{}
	// 		n.Export(&exported)
	// 		fmt.Printf("\nRetrieved Exported Data: %v\n", exported.Title)
	// 	}
	// 	fmt.Println("Result count: ", result.NumDocs)
	// })

	benchmark("filter", func() {
		res := myChunk.Filter(
			`contains(doc.slug, "comic") && doc.price.amount > 600`,
		)
		fmt.Printf("Filter Result: %v\n", len(res))
		for _, doc := range res {
			document := DjaliListing{}
			doc.Export(&document)
			fmt.Printf("Result: %v\n", document.Title)
		}
	})

	// benchmark("retrieval", func() {
	// 	var code string
	// 	for i := 0; i <= 5; i++ {
	// 		code = fmt.Sprintf("mychunk$%v", rand.Intn(len(myChunk.Store)-1))
	// 		fmt.Printf("\nRetrieving %v\n", code)
	// 		data = myChunk.GetAsync(code)
	// 		// exported := User{}
	// 		n := <-data
	// 		// n.Export(&exported)
	// 		exported := n.ExportI().(Result)
	// 		fmt.Printf("Retrieved Exported Data: %v\n\n", exported.Title)

	// 		//benchmark("export", func() {
	// 		//
	// 		//})
	// 	}
	// })

	//fmt.Printf("Retrieved Data: %v\n", data)

	benchmark("commit", func() {
		commitWait = myChunk.CommitAsync()
	})
	<-commitWait
}
