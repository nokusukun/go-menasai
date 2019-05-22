package gomenasai

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	rtypes "github.com/go-ego/riot/types"
	jsoniter "github.com/json-iterator/go"

	"github.com/PaesslerAG/gval"
	"github.com/go-ego/riot"
	"github.com/yalp/jsonpath"
	"gitlab.com/nokusukun/go-menasai/chunk"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type GomenasaiConfig struct {
	Name           string   `json:"name"`
	IndexDir       string   `json:"indexDir"`
	Path           string   `json:"dbpath"`
	ChunkSizeLimit int      `json:"chunkLimit"`
	IndexPaths     []string `json:"indexPaths"`
}

type Gomenasai struct {
	ChunkPaths    []string         `json:"chunkPaths"`
	Configuration *GomenasaiConfig `json:"configuration"`
	ActiveChunk   string           `json:"activeChunk"`

	chunks       map[string]*chunk.Chunk
	indexFilters []jsonpath.FilterFunc
	searchEngine *riot.Engine
	EvalEngine   gval.Language
	lock         *sync.RWMutex
}

func New(config *GomenasaiConfig) (*Gomenasai, error) {
	if doesFileExist(config.Path) {
		return nil, fmt.Errorf("Target directory '%v' already exists, use 'Load' instead", config.Path)
	}
	if config.ChunkSizeLimit == 0 {
		log.Println("Chunksize is not specified or zero, setting to default of 4,096‬")
		config.ChunkSizeLimit = 4096
	}
	if config.IndexDir == "" {
		log.Println("No index directory specified, using defaults")
		config.IndexDir = path.Join(config.Path, "index")
	}
	ensureDir(path.Join(config.Path, "chunks", ".empty"))
	db := &Gomenasai{
		Configuration: config,
		ChunkPaths:    []string{},
	}
	db.WriteConfig()
	db.Initialize()
	return db, nil
}

func Load(dbpath string) (*Gomenasai, error) {
	if !doesFileExist(dbpath) {
		return nil, fmt.Errorf("Target directory '%v' doesn't exist, use 'New' to create a new database", dbpath)
	}
	rawConfig, err := ioutil.ReadFile(path.Join(dbpath, "config.json"))
	if err != nil {
		return nil, fmt.Errorf("Failed to read configuration: ", err)
	}
	db := &Gomenasai{}
	err = json.Unmarshal(rawConfig, db)
	if err != nil {
		return nil, fmt.Errorf("Failed to load configuration to DB: ", err)
	}
	db.Initialize()
	log.Printf("%v contains %v documents.\n", db.Configuration.Name, db.Size())
	return db, nil
}

func (db *Gomenasai) WriteConfig() error {
	gomenJSON, err := json.Marshal(db)
	//log.Println("Saving", string(gomenJSON))
	if err != nil {
		return fmt.Errorf("Failed to save configuration as JSON: %v", err)
	}
	err = ioutil.WriteFile(path.Join(db.Configuration.Path, "config.json"), gomenJSON, 1)
	if err != nil {
		return fmt.Errorf("Failed to write configuration: '%v'", err)
	}
	return nil
}

func (db *Gomenasai) NewChunk() error {
	db.lock.Lock()
	defer db.lock.Unlock()
	config := &chunk.Config{
		IndexPaths: db.Configuration.IndexPaths,
		ID:         fmt.Sprintf("%v-%v", db.Configuration.Name, len(db.chunks)),
	}
	chunkPath := path.Join(db.Configuration.Path, "chunks", config.ID)
	config.Path = chunkPath
	chunk, err := chunk.CreateChunk(config)
	if err != nil {
		return err
	}
	db.chunks[config.ID] = chunk
	db.ChunkPaths = append(db.ChunkPaths, config.Path)
	db.ActiveChunk = config.ID
	chunk.Initialize()
	return nil
}

func (db *Gomenasai) LoadChunk(path string) error {
	chunk, err := chunk.LoadChunk(path)
	if err != nil {
		panic(err)
	}
	chunk.Initialize()
	db.chunks[chunk.Config.ID] = chunk
	return nil
}

func (db *Gomenasai) OverrideEvalEngine(engine gval.Language) {
	db.EvalEngine = engine
}

func (db *Gomenasai) Initialize() {
	db.lock = &sync.RWMutex{}
	db.chunks = make(map[string]*chunk.Chunk)
	db.indexFilters = []jsonpath.FilterFunc{}
	if len(db.ChunkPaths) == 0 {
		err := db.NewChunk()
		if err != nil {
			panic(err)
		}
	} else {
		for _, chunkPath := range db.ChunkPaths {
			err := db.LoadChunk(chunkPath)
			if err != nil {
				panic(err)
			}
		}
	}

	// Initialize extractors for the riot engine
	for _, filterPath := range db.Configuration.IndexPaths {
		filter, _ := jsonpath.Prepare(filterPath)
		db.indexFilters = append(db.indexFilters, filter)
	}

	// Load up the text search engine
	db.searchEngine = &riot.Engine{}
	db.searchEngine.Init(rtypes.EngineOpts{
		NotUseGse:   false,
		UseStore:    true,
		StoreFolder: db.Configuration.IndexDir,
	})

	db.FlushSE()
	go func() {
		for {
			time.Sleep(time.Second * 5)
			db.FlushSE()
		}
	}()

	// Load up the EvaluationEngine for the filters.
	db.EvalEngine = gval.Full(
		gval.Function("contains", func(fullstr string, substr string) bool {
			return strings.Contains(fullstr, substr)
		}),
	)
}

// Flush the search engine index
func (db *Gomenasai) FlushSE() {
	db.searchEngine.FlushIndex()
	db.searchEngine.Flush()
}

func (db *Gomenasai) Search(val string) *GomenasaiSearchResult {
	toreturn := []*chunk.Document{}
	if val == "" {
		for _, c := range db.chunks {
			toreturn = append(toreturn, c.Store...)
		}

	}
	result := db.searchEngine.Search(rtypes.SearchReq{Text: val})
	for _, res := range result.Docs.(rtypes.ScoredDocs) {
		code := res.ScoredID.DocId
		data, err := db.Get(code)
		if err == nil {
			toreturn = append(toreturn, data)
		} else {
			log.Printf("Failed to retrieve from index '%v': %v\n", data, err)
		}
	}
	return &GomenasaiSearchResult{
		Documents: toreturn,
		Manager:   db,
	}
}

func (db *Gomenasai) insertOneIndex(id string, index string) {
	db.searchEngine.Index(id, rtypes.DocData{Content: index})
}

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
	db.lock.Lock()
	activeChunk := db.chunks[db.ActiveChunk]
	res, asJSON, err := activeChunk.Insert(value)
	db.lock.Unlock()

	db.InsertIndex(res, asJSON)

	// Prevents race condition where the goroutine thinks it's still in the previous
	// chunk even if it's a new chunk.
	activeChunkID := activeChunk.Config.ID
	// Runs a goroutine for the additional, non time critical stuff.
	if db.chunks[activeChunkID].StoreCount() >= db.Configuration.ChunkSizeLimit {
		db.NewChunk()
	}

	return
	// return res.Content.(string), res.Error
}

func (db *Gomenasai) Get(id string) (*chunk.Document, error) {
	idElems := strings.Split(id, "$")
	if len(idElems) != 2 {
		return nil, fmt.Errorf("Invalid document ID '%v'", id)
	}
	chunkID := idElems[0]
	activeChunk := db.chunks[chunkID]
	res := <-activeChunk.GetAsync(id)
	return res.Content.(*chunk.Document), res.Error
}

func (db *Gomenasai) Commit() {
	chunkCount := len(db.chunks)
	toreturns := make(chan error, chunkCount)
	for id, chunk := range db.chunks {
		log.Println("Sending Commit message to:", id)
		result := chunk.CommitAsync()
		go func() {
			err := <-result
			toreturns <- err
		}()
	}

	for i := 0; i <= chunkCount-1; i++ {
		err := <-toreturns
		if err != nil {
			log.Println("Failed to save a chunk:", err)
		}
	}
	db.WriteConfig()

}

func (db *Gomenasai) Close() {
	db.Commit()
	db.FlushSE()
	db.searchEngine.Close()
}

func (db *Gomenasai) Size() int {
	var total int
	for _, c := range db.chunks {
		total += c.StoreCount()
	}
	return total
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
