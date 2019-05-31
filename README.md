# go-menasai

A compact performant nosql store with full text searching.
* Compact Binary
* Full featured text searching
* Extensible filtering and sorting (handled by gval)

## Install
```bash
~$ go get -u gitlab.com/nokusukun/go-menasai
```

## Usage
Godoc: https://godoc.org/gitlab.com/nokusukun/go-menasai/manager
```golang
    var db *gomenasai.Gomenasai
    // Check if the database exists
    if gomenasai.Exists("~/.testDB") {
        // Load the database
        db, err := gomenasai.Load("~/.testDB")
    } else {
        // Create a new database.
        db, err := gomenasai.New(&gomenasai.GomenasaiConfig{
                // Database label
            Name:           "TestDB",                   
                // Database path
            Path:           "~/.testDB",                
                // How much entries before
                //      a new chunk gets created.
            ChunkSizeLimit: 1024,                       
                // JSON paths of the properties to get added to the index. 
            IndexPaths:     []string{"$.title", "$.description"}, 
        })
    }

	if err != nil {
		panic(err)
    }
    
    // A struct with json defined fields
    myObject := SampleObject{.....}

    // Insert data to the database
    id, _ := db.Insert(post)
    // id: TestDB-0$1       // The document ID
    
    // Retrieving data by ID
    doc := db.Get(id)
    
    // Similar to json.Unmarshal
    emptyObject := SampleObject{}
    doc.Export(&emptyObject)

    // Searching
    result := db.Search("watch").
		Filter(`contains(doc.slug, "rolex")`).
		Filter(`contains(doc.slug, "daytona")`).
		Sort(`x.price.amount < y.price.amount`)

    // db.Search() returns a *gomenasai.GomenasaiSearchResult object 
    //      containing documents which matches on the index where 
    //      you can do more operations to the results returned.

    log.Println("Search and sort result for 'watch' filtered by 'watch': ", len(result.Documents))

    // Exports the database into a JSONArray in which can be returned as an API response. 
    jsonData, _ := result.ExportJSONArray()
    fmt.Fprint(*http.ResponseWriter, jsonData)


    // Updating
    rdoc, err := manager.Get("TestDB-0$1")

	if err != nil {
		t.Errorf("Failed to get document: %v", err)
	}

	document := SampleDocument{}
	rdoc.Export(&document)

	document.Title = "New Title"

    err = manager.Update(rdoc.ID, &document)
    
    // Delete
    err = manager.Delete("TestDB-0$1")

	if err != nil {
		t.Errorf("Failed to delete document: %v", err)
	}

```


## Todo
* More result manipulation