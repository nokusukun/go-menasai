package chunk

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"

	"github.com/PaesslerAG/gval"
	"github.com/go-ego/riot"
	rtypes "github.com/go-ego/riot/types"

	jsoniter "github.com/json-iterator/go"
	"github.com/yalp/jsonpath"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type Chunk struct {
	Store        []*Document `json:"documents"`
	Config       *Config     `json:"config"`
	aRunning     bool
	aJobs        chan func()
	indexFilters []jsonpath.FilterFunc
	searchEngine *riot.Engine
	EvalEngine   gval.Language
}

type Config struct {
	IndexPaths []string `json:"indexPaths"`
	ID         string   `json:"chunkId"`
	Path       string   `json:"path"`
	IndexDir   string   `json:"indexDir"`
}

type Document struct {
	ID      string `json:"id"`
	Content []byte `json:"body"`
}

// CreateChunk - Creates a chunk based on a specified id and path,
// 	returns an error when the path already exists.
func CreateChunk(config *Config) (*Chunk, error) {
	if _, err := os.Stat(config.Path); err == nil {
		return nil, fmt.Errorf("'%v' already exists, failed to create chunk", config.Path)
	}
	newChunk := &Chunk{Config: config}
	chunkJSON, err := json.Marshal(newChunk)
	if err != nil {
		return nil, err
	}
	ioutil.WriteFile(config.Path, chunkJSON, 1)
	newChunk.Initialize()
	return newChunk, nil
}

// LoadChunk loads an already existing chunk file.
func LoadChunk(path string) (*Chunk, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, fmt.Errorf("'%v' does not exist, failed to load chunk", path)
	}
	chunkBytes, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	newChunk := &Chunk{}
	json.Unmarshal(chunkBytes, newChunk)
	newChunk.Initialize()
	return newChunk, nil
}

// FlushSE flushes the index
func (c *Chunk) FlushSE() {
	c.searchEngine.Flush()
}

// SearchIndex searches the search engine for a specified string.
//		Incomplete implementation, todo implement the rest
func (c *Chunk) SearchIndex(val string) []*Document {
	toreturn := []*Document{}
	result := c.searchEngine.Search(rtypes.SearchReq{Text: val})
	for _, res := range result.Docs.(rtypes.ScoredDocs) {
		code := res.ScoredID.DocId
		data := c.GetAsync(code)
		toreturn = append(toreturn, <-data)
	}

	return toreturn
}

func (c *Chunk) insertOneIndex(id string, value string) {
	//searcher.Index("1", types.DocData{Content: text})
	// fmt.Println("Indexing", id, value)
	c.searchEngine.Index(id, rtypes.DocData{Content: value})
}

func (c *Chunk) OverrideEvalEngine(engine gval.Language) {
	c.EvalEngine = engine
}

// Initialize - Initializes the chunk services, like the search handler.
//	Should be managed by the chunk manager.
// 	TODO - Chunk manager should be the one managing the search engine.
func (c *Chunk) Initialize() {
	c.runAsyncScheduler()
	for _, filterPath := range c.Config.IndexPaths {
		filter, _ := jsonpath.Prepare(filterPath)
		c.indexFilters = append(c.indexFilters, filter)
	}

	c.searchEngine = &riot.Engine{}
	c.searchEngine.Init(rtypes.EngineOpts{
		NotUseGse:   false,
		UseStore:    true,
		StoreFolder: c.Config.IndexDir,
	})

	c.EvalEngine = gval.Full(
		gval.Function("contains", func(fullstr string, substr string) bool {
			return strings.Contains(fullstr, substr)
		}),
	)
}

func (c *Chunk) Filter(query string) []*Document {
	toreturn := []*Document{}
	eval, err := c.EvalEngine.NewEvaluable(fmt.Sprintf("%v", query))

	if err != nil {
		fmt.Println(err)
	}

	for _, doc := range c.Store {
		mapdoc := make(map[string]interface{})
		json.Unmarshal(doc.Content, &mapdoc)
		value, err := eval(context.Background(), map[string]interface{}{
			"doc": mapdoc,
		})
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println(value)

		val, ok := value.(bool)
		if ok {
			if val {
				toreturn = append(toreturn, doc)
			}
		}
	}
	return toreturn
}

// StoreCount returns how many items are in the store
func (c *Chunk) StoreCount() int {
	return len(c.Store)
}

// runAsyncScheduler runs the scheduler service that multiplexes the
//		processes to run on only one goroutine.
//		Slower but extremely thread safe.
func (c *Chunk) runAsyncScheduler() {
	c.aJobs = make(chan func(), 1000)
	go func() {
		for job := range c.aJobs {
			job()
		}
	}()
}

func (c *Chunk) makeID() string {
	storeCount := c.StoreCount()
	if storeCount == 0 {
		return fmt.Sprintf("%v$%v", c.Config.ID, 1)
	}
	lastDoc := c.Store[storeCount-1]
	x := strings.Split(lastDoc.ID, "$")
	nextID, _ := strconv.Atoi(x[1])
	code := fmt.Sprintf("%v$%v", c.Config.ID, nextID+1)
	//fmt.Println(code)
	return code
}

// Insert - Inserts an interface to the database
func (c *Chunk) Insert(value interface{}, addToIndex ...bool) (string, error) {
	var addIndex bool
	if len(addToIndex) > 0 {
		addIndex = addToIndex[0]
	}

	ID := c.makeID()
	asJSON, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	doc := Document{ID: ID, Content: asJSON}
	c.Store = append(c.Store, &doc)

	// Add to index
	if addIndex {
		// Unmarshals the marshalled json into a generic map[interface{}]
		// the filter only works on that.
		var eInterf interface{}
		json.Unmarshal(asJSON, &eInterf)
		indices := []string{}
		// Loop through the chunk's index filters and append it to
		//		the text search engine
		for _, indexFilter := range c.indexFilters {
			jval, err := indexFilter(eInterf)
			if err == nil {
				indices = append(indices, fmt.Sprintf("%v", jval))
			} else {
				fmt.Printf("Indexing failed: %v\n", err)
			}
		}
		c.insertOneIndex(ID, fmt.Sprint(indices))
	}

	return ID, nil
}

// InsertAsync - Asynchronously inserts data to the database, returns a channel with the ID
func (c *Chunk) InsertAsync(value interface{}, addToIndex ...bool) chan string {
	result := make(chan string, 1)
	c.aJobs <- func() {
		res, err := c.Insert(value, addToIndex...)
		if err != nil {
			result <- ""
		}
		result <- res
		close(result)
	}
	return result
}

// Gets retrieves a document. Non thread safe.
func (c *Chunk) Get(id string) *Document {
	for _, doc := range c.Store {
		if doc.ID == id {
			return doc
		}
	}
	return &Document{}
}

// GetsAsync retrieves a document, returns a channel to recieve the document. Thread safe.
func (c *Chunk) GetAsync(id string) chan *Document {
	result := make(chan *Document, 1)
	c.aJobs <- func() {
		res := c.Get(id)
		result <- res
		close(result)
	}
	return result
}

// Commit immediately writes the contents to the file. Not thread safe.
func (c *Chunk) Commit() error {
	chunkJSON, err := json.Marshal(c)
	if err != nil {
		return err
	}
	ioutil.WriteFile(c.Config.Path, chunkJSON, 1)
	return nil
}

// CommitAsync waits for the pending write and get functions to finish before
//		writing the contents to the file. Thread safe.
func (c *Chunk) CommitAsync() chan error {
	errorChannel := make(chan error)
	go func() {
		errorChannel <- c.Commit()
	}()
	return errorChannel
}

// Export exports the Document to a specified interface, like json.Unmarshal
func (d *Document) Export(interf interface{}) {
	json.Unmarshal(d.Content, interf)
}

// ExportI returns the document as an interface{}, cast it using `result.(OriginalType)`
func (d *Document) ExportI() interface{} {
	return d.Content
}

// MarshalJSON is used by the internal marshaller.
func (d *Document) MarshalJSON() ([]byte, error) {
	return json.Marshal(Document{
		d.ID,
		d.Content,
	})
}

// TransDocArrtoJSON transforms an array of documents to JSON
func TransDocArrtoJSON(documents []*Document) ([]byte, error) {
	results := []interface{}{}
	for _, doc := range documents {
		tmpinterf := new(interface{})
		json.Unmarshal(doc.Content, &tmpinterf)
		results = append(results, tmpinterf)
	}
	return json.Marshal(results)
}

// FilterCollection filters a collection
func (c *Chunk) FilterCollection(collection []*Document, query string) []*Document {
	fmt.Println("Loading Filter", query)
	query = fmt.Sprintf("%v", query)
	toreturn := []*Document{}
	eval, err := c.EvalEngine.NewEvaluable(query)

	if err != nil {
		fmt.Println("Failed to load filter: ", err)
	}

	for _, doc := range collection {
		mapdoc := make(map[string]interface{})
		json.Unmarshal(doc.Content, &mapdoc)

		value, err := eval(context.Background(), map[string]interface{}{
			"doc": mapdoc,
		})

		if err != nil {
			fmt.Println(err)
		}

		val, ok := value.(bool)
		if ok {
			if val {
				toreturn = append(toreturn, doc)
			}
		}
	}
	return toreturn
}
