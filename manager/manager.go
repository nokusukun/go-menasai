package gomenasai

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/blevesearch/bleve"
	jsoniter "github.com/json-iterator/go"
	bolt "go.etcd.io/bbolt"

	"github.com/PaesslerAG/gval"

	"github.com/yalp/jsonpath"

	"github.com/nokusukun/go-menasai/chunk"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

// GomenasaiConfig is passed to gomenasai.New
type GomenasaiConfig struct {
	Name           string   `json:"name"`
	IndexDir       string   `json:"indexDir"`
	Path           string   `json:"dbpath"`
	ChunkSizeLimit int      `json:"chunkLimit"` // todo - ffd:chunk
	IndexPaths     []string `json:"indexPaths"`
	NoIndex        bool     `json:"noIndex"`
	BoltPath       string   `json:"bolt_path"`
}

// Gomenasai is a service that manages individual chunks
type Gomenasai struct {
	ChunkPaths    []string         `json:"chunkPaths"` // todo - ffd:chunk
	Configuration *GomenasaiConfig `json:"configuration"`
	ActiveChunk   string           `json:"activeChunk"` // todo - ffd:chunk

	bolt *bolt.DB

	//chunks       map[string]*chunk.Chunk
	indexFilters []jsonpath.FilterFunc
	searchEngine bleve.Index
	EvalEngine   gval.Language
	lock         *sync.RWMutex
}

// Exists checks if a database exists in a specified path.
func Exists(dbpath string) bool {
	if !doesFileExist(dbpath) {
		return false
	}
	rawConfig, err := ioutil.ReadFile(path.Join(dbpath, "config.json"))
	if err != nil {
		return false
	}

	if rawConfig != nil {
		return true
	}

	return true

}

// New retuns a new Gomenasai store, returns an error when the database
// already exists. Use `gomenasai.Load` instead.
func New(config *GomenasaiConfig) (*Gomenasai, error) {
	if doesFileExist(config.Path) {
		return nil, fmt.Errorf("target directory '%v' already exists, use 'Load' instead", config.Path)
	}
	// todo - ffd:chunk
	if config.ChunkSizeLimit == 0 {
		log.Println("Chunksize is not specified or zero, setting to default of 4,096‬")
		config.ChunkSizeLimit = 4096
	}
	if config.IndexDir == "" {
		log.Println("No index directory specified, using defaults")
		config.IndexDir = path.Join(config.Path, "index")
	}
	if config.BoltPath == "" {
		log.Println("BoltPath not specified, using defaults")
		config.BoltPath = path.Join(config.Path, "store")
	}
	ensureDir(path.Join(config.Path, "chunks", ".empty"))
	db := &Gomenasai{
		Configuration: config,
		ChunkPaths:    []string{},
	}
	db.WriteState()
	db.Initialize()
	return db, nil
}

// Load returns an existing closed Gomenasai store, retuns an error when it doesn't exist.
// Use `gomenasai.New` instead.
func Load(dbpath string) (*Gomenasai, error) {
	if !doesFileExist(dbpath) {
		return nil, fmt.Errorf("target directory '%v' doesn't exist, use 'New' to create a new database", dbpath)
	}
	rawConfig, err := ioutil.ReadFile(path.Join(dbpath, "config.json"))
	if err != nil {
		return nil, fmt.Errorf("failed to read configuration: %v", err)
	}
	db := &Gomenasai{}
	err = json.Unmarshal(rawConfig, db)
	if err != nil {
		return nil, fmt.Errorf("failed to load configuration to DB: %v", err)
	}
	db.Initialize()
	log.Printf("%v contains %v documents.\n", db.Configuration.Name, db.Size())
	return db, nil
}

// WriteState commits the current database state to config.json
func (db *Gomenasai) WriteState() error {
	gomenJSON, err := json.Marshal(db)
	//log.Println("Saving", string(gomenJSON))
	if err != nil {
		return fmt.Errorf("failed to save configuration as JSON: %v", err)
	}
	err = ioutil.WriteFile(path.Join(db.Configuration.Path, "config.json"), gomenJSON, 0777)
	if err != nil {
		return fmt.Errorf("failed to write configuration: '%v'", err)
	}
	return nil
}

// NewChunk generates an new chunk and sets it as the active chunk.
//	Called by the store, this should be private.
//func (db *Gomenasai) NewChunk() error {
//	db.lock.Lock()
//	defer db.lock.Unlock()
//	config := &chunk.Config{
//		IndexPaths: db.Configuration.IndexPaths,
//		ID:         fmt.Sprintf("%v-%v", db.Configuration.Name, len(db.chunks)),
//	}
//	chunkPath := path.Join(db.Configuration.Path, "chunks", config.ID)
//	config.Path = chunkPath
//	c, err := chunk.CreateChunk(config)
//	if err != nil {
//		return err
//	}
//	db.chunks[config.ID] = c
//	db.ChunkPaths = append(db.ChunkPaths, config.Path)
//	db.ActiveChunk = config.ID
//	// c.Initialize()
//	db.WriteState()
//	return nil
//}

// LoadChunk loads an existing chunk. Called by
// gomenasai.Initialize.
//func (db *Gomenasai) LoadChunk(xpath string) error {
//	c, err := chunk.LoadChunk(xpath)
//	if err != nil {
//		panic(err)
//	}
//	// chunk.Initialize()
//	_, chunkID := path.Split(xpath)
//	db.chunks[chunkID] = c
//	return nil
//}

// OverrideEvalEngine lets you override and add functions accessible
//	to the Filter.
func (db *Gomenasai) OverrideEvalEngine(engine gval.Language) {
	db.EvalEngine = engine
}

// Initialize loads the entire database state, should never be called
// outside since it's only handled by `New()` and `Load()`.
func (db *Gomenasai) Initialize() {
	db.lock = &sync.RWMutex{}
	// TODO - Remove all references to chunks
	//db.chunks = make(map[string]*chunk.Chunk)
	db.indexFilters = []jsonpath.FilterFunc{}

	// TODO - Remove all references to chunks
	//if len(db.ChunkPaths) == 0 {
	//	err := db.NewChunk()
	//	if err != nil {
	//		panic(err)
	//	}
	//} else {
	//	for _, chunkPath := range db.ChunkPaths {
	//		err := db.LoadChunk(chunkPath)
	//		if err != nil {
	//			panic(err)
	//		}
	//	}
	//}

	bdb, err := bolt.Open(db.Configuration.BoltPath, 0600, nil)
	if err != nil {
		panic(fmt.Errorf("failed to open boltdb '%v': %v", db.Configuration.BoltPath, err))
	}
	db.bolt = bdb

	db.bolt.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("default"))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})

	for _, filterPath := range db.Configuration.IndexPaths {
		filter, _ := jsonpath.Prepare(filterPath)
		db.indexFilters = append(db.indexFilters, filter)
	}

	if !db.Configuration.NoIndex {
		idxPath := path.Join(db.Configuration.Path, "index.db")
		dbAlreadyExists := doesFileExist(idxPath)
		// fmt.Println(idxPath)

		if !dbAlreadyExists {
			mapping := bleve.NewIndexMapping()
			index, err := bleve.New(idxPath, mapping)
			if err != nil {
				panic(err)
			}
			db.searchEngine = index
		} else {
			index, err := bleve.Open(idxPath)
			if err != nil {
				panic(err)
			}
			db.searchEngine = index
		}

	}

	// Load up the EvaluationEngine for the filters.
	db.EvalEngine = gval.Full(
		gval.Function("contains", func(fullstr string, substr string) bool {
			return strings.Contains(fullstr, substr)
		}),
	)
}

// FlushSE (DEPRECATED) flushes the search engine index
func (db *Gomenasai) FlushSE() {
	// does nothing now, but is being kept for compatibility
}

// Search searches the index for a query string, returns a search object
func (db *Gomenasai) Search(val string) *GomenasaiSearchResult {
	var toReturn []chunk.Document

	if val == "" || db.Configuration.NoIndex {
		//for _, c := range db.chunks {
		//	for _, val := range c.Store {
		//		if val != nil {
		//			toReturn = append(toReturn, *val)
		//		}
		//	}
		//}
		db.bolt.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("default"))
			c := b.Cursor()

			for k, v := c.First(); k != nil; k, v = c.Next() {
				toReturn = append(toReturn, Byte2Document(v))
			}

			return nil
		})
		return &GomenasaiSearchResult{
			Documents: toReturn,
			Manager:   db,
			Count:     len(toReturn),
		}
	}
	//result := db.searchEngine.Search(rtypes.SearchReq{Text: val})

	//query := bleve.NewFuzzyQuery(val)
	query := bleve.NewMatchQuery(val)
	query.SetFuzziness(2)
	search := bleve.NewSearchRequest(query)
	searchResults, err := db.searchEngine.Search(search)

	if err != nil {
		panic(err)
	}
	//for _, hit := range searchResults.Hits {
	//	data, err := db.Get(hit.ID)
	//	if err == nil && data != nil {
	//		toReturn = append(toReturn, *data)
	//	} else {
	//		log.Printf("Failed to retrieve from index '%v': %v\n", hit.ID, err)
	//	}
	//}

	db.bolt.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("default"))

		for _, hit := range searchResults.Hits {
			d := b.Get([]byte(hit.ID))
			if len(d) != 0 {
				toReturn = append(toReturn, Byte2Document(d))
			}
		}

		return nil
	})

	return &GomenasaiSearchResult{
		Documents: toReturn,
		Manager:   db,
		Count:     len(toReturn),
	}
}

func (db *Gomenasai) deleteFromIndex(id string) {
	db.searchEngine.Delete(id)
}

func (db *Gomenasai) insertOneIndex(id string, index string) {
	db.searchEngine.Index(id, index)
}

// InsertIndex Creates a new index based on the filters specified
//  on the IndexPaths
func (db *Gomenasai) InsertIndex(ID string, asJSON []byte) {
	var eInterf interface{}
	json.Unmarshal(asJSON, &eInterf)
	indices := []string{}
	// Loop through the chunk's index filters and append it to
	//		the text search engine
	for _, indexFilter := range db.indexFilters {
		jval, err := indexFilter(eInterf)
		if err == nil {
			indices = append(indices, fmt.Sprintf("%v", jval))
		} else {
			log.Printf("Indexing failed: %v\n", err)
		}
	}
	db.insertOneIndex(ID, fmt.Sprint(indices))
}

// Insert inserts a struct to the database and adds the document to the index.
func (db *Gomenasai) Insert(value interface{}) (res string, err error) {
	//db.lock.Lock()
	//activeChunk := db.chunks[db.ActiveChunk]
	//res, asJSON, err := activeChunk.Insert(value)
	//db.lock.Unlock()
	var ID string

	asJSON, err := json.Marshal(value)
	if err != nil {
		return "", err
	}

	err = db.bolt.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("default"))
		nextseq, _ := b.NextSequence()
		ID = strconv.FormatUint(nextseq, 32)
		doc := chunk.Document{ID: ID, Content: asJSON}

		j, _ := doc.MarshalJSON()
		err := b.Put([]byte(ID), j)

		if err != nil {
			return fmt.Errorf("panic: '%v', %v", "err := b.Put([]byte(ID), j)", err)
		}
		return nil
	})

	if err != nil {
		return "", err
	}

	db.InsertIndex(ID, asJSON)

	//// Prevents race condition where the goroutine thinks it's still in the previous
	//// chunk even if it's a new chunk.
	//activeChunkID := activeChunk.Config.ID
	//// Runs a goroutine for the additional, non time critical stuff.
	//if db.chunks[activeChunkID].StoreCount() >= db.Configuration.ChunkSizeLimit {
	//	db.NewChunk()
	//}

	return ID, nil
	// return res.Content.(string), res.Error
}

// Get retuns a Document specified by the DocumentID
func (db *Gomenasai) Get(id string) (*chunk.Document, error) {
	//idElems := strings.Split(id, "$")
	//if len(idElems) != 2 {
	//	return nil, fmt.Errorf("invalid document ID '%v'", id)
	//}
	//chunkID := idElems[0]
	//activeChunk := db.chunks[chunkID]
	//res := <-activeChunk.GetAsync(id)
	//if res.Content == nil {
	//	return &chunk.Document{}, res.Error
	//}
	//return res.Content.(*chunk.Document), res.Error

	toRet := chunk.Document{}
	_ = db.bolt.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("default"))
		toRet = Byte2Document(b.Get([]byte(id)))

		return nil
	})

	return &toRet, nil
}

// Delete deletes a document specified by the document ID
func (db *Gomenasai) Delete(id string) error {
	//db.lock.Lock()
	//defer db.lock.Unlock()
	//
	//idElems := strings.Split(id, "$")
	//if len(idElems) != 2 {
	//	return fmt.Errorf("invalid document ID '%v'", id)
	//}
	//chunkID := idElems[0]
	//db.deleteFromIndex(id)
	//activeChunk := db.chunks[chunkID]
	//return activeChunk.Delete(id)
	return db.bolt.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("default"))
		err := b.Delete([]byte(id))
		if err != nil {
			return err
		}
		return nil
	})
}

// Update updates the document specified by a document ID
func (db *Gomenasai) Update(docId string, content interface{}) error {
	//db.lock.Lock()
	//defer db.lock.Unlock()
	//
	//idElems := strings.Split(docId, "$")
	//if len(idElems) != 2 {
	//	return fmt.Errorf("invalid document ID '%v'", docId)
	//}
	//chunkID := idElems[0]
	//activeChunk := db.chunks[chunkID]
	//
	//asJSON, err := activeChunk.Update(docId, content)

	return db.bolt.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("default"))

		asJSON, _ := json.Marshal(content)

		doc := chunk.Document{ID: docId, Content: asJSON}

		j, _ := doc.MarshalJSON()
		err := b.Put([]byte(docId), j)

		if err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("panic: '%v', %v", "err := b.Put([]byte(ID), j)", err)
		}
		db.InsertIndex(docId, asJSON)
		return nil
	})
}

// Commit saves the database state to the disk. Signals the chunks to
//	offload the data to the disks as well. Waits for all of them to finish.
func (db *Gomenasai) Commit() {
	//chunkCount := len(db.chunks)
	//toreturns := make(chan error, chunkCount)
	//for id, c := range db.chunks {
	//	log.Println("Sending Commit message to:", id)
	//	result := c.CommitAsync()
	//	go func() {
	//		err := <-result
	//		toreturns <- err
	//	}()
	//}
	//
	//for i := 0; i <= chunkCount-1; i++ {
	//	err := <-toreturns
	//	if err != nil {
	//		log.Println("Failed to save a chunk:", err)
	//	}
	//}

	_ = db.WriteState()

}

// Close closes all of the running services and commits everything to disk.
func (db *Gomenasai) Close() {
	db.Commit()
	db.bolt.Close()
	db.searchEngine.Close()
}

// Size returns the size of the current database.
func (db *Gomenasai) Size() uint64 {
	//var total int
	//for _, c := range db.chunks {
	//	total += c.StoreCount()
	//}
	count, _ := db.searchEngine.DocCount()
	return count
}

func ensureDir(fileName string) {
	dirName := filepath.Dir(fileName)
	if _, serr := os.Stat(dirName); serr != nil {
		merr := os.MkdirAll(dirName, os.ModePerm)
		if merr != nil {
			panic(merr)
		}
	}
}

func doesFileExist(name string) bool {
	if _, err := os.Stat(name); err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
}

func inStrArray(array []string, str string) bool {
	for _, elem := range array {
		if elem == str {
			return true
		}
	}
	return false
}

// Checks if it's actually an error
func isActuallyAnError(err error) bool {
	if err != nil {
		if err.Error() != "not an error" {
			return true
		}
	}
	return false
}
