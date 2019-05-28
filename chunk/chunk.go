package chunk

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"

	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type Chunk struct {
	Store          map[string]*Document `json:"documents"`
	Config         *Config              `json:"config"`
	LastDocumentID string               `json:"lastdocadd"`
	aRunning       bool
	aJobs          chan func()
}

type Config struct {
	IndexPaths []string `json:"indexPaths"`
	ID         string   `json:"chunkId"`
	Path       string   `json:"path"`
	IndexDir   string   `json:"indexDir"`
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
	newChunk.Store = make(map[string]*Document)
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

// Initialize - Initializes the chunk services, like the search handler.
//	Should be managed by the chunk manager.
// 	TODO - Chunk manager should be the one managing the search engine.
func (c *Chunk) Initialize() {

	c.runAsyncScheduler()
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
		c.LastDocumentID = fmt.Sprintf("%v$%v", c.Config.ID, 1)
		return c.LastDocumentID
	}
	lastDoc := c.LastDocumentID
	x := strings.Split(lastDoc, "$")
	nextID, _ := strconv.Atoi(x[1])
	code := fmt.Sprintf("%v$%v", c.Config.ID, nextID+1)
	//fmt.Println(code)
	c.LastDocumentID = code
	return code
}

// Insert - Inserts an interface to the database
func (c *Chunk) Insert(value interface{}) (string, []byte, error) {

	ID := c.makeID()
	asJSON, err := json.Marshal(value)
	if err != nil {
		return "", nil, err
	}
	doc := Document{ID: ID, Content: asJSON}
	//c.Store = append(c.Store, &doc)
	c.Store[ID] = &doc

	return ID, asJSON, nil
}

type ReturnAsync struct {
	Content interface{}
	Error   error
}

// InsertAsync - Asynchronously inserts data to the database, returns a channel with the ID
func (c *Chunk) InsertAsync(value interface{}) chan *ReturnAsync {
	result := make(chan *ReturnAsync, 1)
	c.aJobs <- func() {
		res, _, err := c.Insert(value)
		result <- &ReturnAsync{
			Content: res,
			Error:   err,
		}

		close(result)
	}
	return result
}

// Get retrieves a document. Non thread safe.
func (c *Chunk) Get(id string) *Document {
	//for _, doc := range c.Store {
	//	if doc.ID == id {
	//		return doc
	//	}
	//}
	//return nil
	doc := c.Store[id]

	return doc
}

// GetAsync retrieves a document, returns a channel to recieve the document. Thread safe.
func (c *Chunk) GetAsync(id string) chan *ReturnAsync {
	result := make(chan *ReturnAsync, 1)
	c.aJobs <- func() {
		res := c.Get(id)
		toreturn := &ReturnAsync{}
		if res == nil {
			toreturn.Error = fmt.Errorf("No document found")
		} else {
			toreturn.Content = res
			toreturn.Error = nil
		}
		result <- toreturn
		close(result)
	}
	return result
}

// Update changes the content of an ID
func (c *Chunk) Update(ID string, content interface{}) ([]byte, error) {
	asJSON, err := json.Marshal(content)
	if err != nil {
		return nil, err
	}
	doc := Document{ID: ID, Content: asJSON}
	//c.Store = append(c.Store, &doc)
	c.Store[ID] = &doc

	return asJSON, nil
}

// Delete deletes a Document ID from the chunk.
func (c *Chunk) Delete(id string) error {
	delete(c.Store, id)
	if c.Store[id] != nil {
		return fmt.Errorf("Failed to delete document: %v", c.Store[id])
	}
	return nil
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
