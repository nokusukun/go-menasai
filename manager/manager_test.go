package gomenasai_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	gomenasai "gitlab.com/nokusukun/go-menasai/manager"
)

type SampleDocument struct {
	PeerSlugID         string        `json:"peerSlugId"`
	ParentPeer         string        `json:"parentPeer"`
	RawData            string        `json:"rawData"`
	AcceptedCurrencies []string      `json:"acceptedCurrencies"`
	AverageRating      int64         `json:"averageRating"`
	Categories         []string      `json:"categories"`
	CoinType           string        `json:"coinType"`
	ContractType       string        `json:"contractType"`
	Description        string        `json:"description"`
	Hash               string        `json:"hash"`
	Language           string        `json:"language"`
	Moderators         []interface{} `json:"moderators"`
	Nsfw               bool          `json:"nsfw"`
	Price              Price         `json:"price"`
	RatingCount        int64         `json:"ratingCount"`
	Slug               string        `json:"slug"`
	Thumbnail          Thumbnail     `json:"thumbnail"`
	Title              string        `json:"title"`
}

type Price struct {
	Amount       int64  `json:"amount"`
	CurrencyCode string `json:"currencyCode"`
	Modifier     int64  `json:"modifier"`
}

type Thumbnail struct {
	Medium string `json:"medium"`
	Small  string `json:"small"`
	Tiny   string `json:"tiny"`
}

func TestCleanup(t *testing.T) {
	removeContents := func(dir string) error {
		d, err := os.Open(dir)
		if err != nil {
			return err
		}
		defer d.Close()
		names, err := d.Readdirnames(-1)
		if err != nil {
			return err
		}
		for _, name := range names {
			err = os.RemoveAll(filepath.Join(dir, name))
			if err != nil {
				return err
			}
		}
		os.RemoveAll(dir)
		return nil
	}

	err := removeContents("test_database")
	if err != nil {
		fmt.Println("No test database found.")
	}
}

func TestManager(t *testing.T) {

	manager, err := gomenasai.New(&gomenasai.GomenasaiConfig{
		Name:       "TestDB",
		Path:       "test_database",
		IndexPaths: []string{"$.title", "$.description", "$.hash"},
	})

	defer manager.Close()

	if err != nil {
		t.Error(err)
	}

	fmt.Print(manager.Configuration.Name)

	// Output:
	// TestDB

}

func TestInsert(t *testing.T) {

	manager, err := gomenasai.Load("test_database")

	defer manager.Close()

	if err != nil {
		t.Error(err)
	}

	sampleJSON := `{
        "peerSlugId": "QmNsxewo55EemqcBAXQx3jozkENBkujAHm8WRyrTpHZ4fL:live-plant-san-pedro-cactus-trichocereus-bridgesii",
        "parentPeer": "QmNsxewo55EemqcBAXQx3jozkENBkujAHm8WRyrTpHZ4fL",
        "rawData": "",
        "acceptedCurrencies": ["BTC", "BCH", "LTC", "ZEC"],
        "averageRating": 0,
        "categories": ["Cactus Live Plants"],
        "coinType": "",
        "contractType": "PHYSICAL_GOOD",
        "description": "\u003cp\u003e\u003cspan\u003eYou will receive 1 x TRICHOCEREUS BRIDGESII (also known as \u0026#34;BOLIVIAN TORCH\u0026#34; or ECHINOPSIS LAGENIFORMIS), similar to the one on the photo.\u003c/span\u003e\u0026lt;/",
        "hash": "Qmadgec6CBnGMhEyUurjey5tqkKHv935na5e5oxa1U4QKa",
        "language": "",
        "moderators": [],
        "nsfw": false,
        "price": {
            "amount": 899,
            "currencyCode": "USD",
            "modifier": 0
        },
        "ratingCount": 0,
        "slug": "live-plant-san-pedro-cactus-trichocereus-bridgesii",
        "thumbnail": {
            "medium": "zb2rhkkGAeibDYSyLRpw4b4R5DcR3J9iw1qskmBZaU9wQK1iK",
            "small": "zb2rhf24mzS2XB8EGceqDnPobQZ7K2KUnJAJqNj2sLa1krTEp",
            "tiny": "zb2rhdS4WPgYAFgzBKvQC3CRFxsFgSeRV7ofwE7YWSac9FdiJ"
        },
        "title": "Live plant Bolivian Torch cactus (Trichocereus Bridgesii)"
	}`

	sampleDoc := SampleDocument{}
	json.Unmarshal([]byte(sampleJSON), &sampleDoc)
	documentId, err := manager.Insert(sampleDoc)

	if err != nil {
		t.Errorf("Failed to insert document: %v", err)
	}

	expected := "TestDB-0$1"

	if documentId != expected {
		t.Errorf("Unexpected document ID. Expected '%v' got '%v'", expected, documentId)
	}

}

func TestGet(t *testing.T) {
	manager, err := gomenasai.Load("test_database")

	defer manager.Close()

	if err != nil {
		t.Error(err)
	}

	rdoc, err := manager.Get("TestDB-0$1")

	if err != nil {
		t.Errorf("Failed to get document: %v", err)
	}

	document := SampleDocument{}
	rdoc.Export(&document)

	expected := "Live plant Bolivian Torch cactus (Trichocereus Bridgesii)"

	if document.Title != expected {
		t.Errorf("Unexpected document title. Expected '%v' got '%v'", expected, document.Title)
	}
}

func TestUpdate(t *testing.T) {
	manager, err := gomenasai.Load("test_database")
	defer manager.Close()

	rdoc, err := manager.Get("TestDB-0$1")

	if err != nil {
		t.Errorf("Failed to get document: %v", err)
	}

	document := SampleDocument{}
	rdoc.Export(&document)

	document.Title = "New Title"

	err = manager.Update(rdoc.ID, &document)

	manager.Commit()

	if err != nil {
		t.Errorf("Failed to update document: %v", err)
	}

	ndoc, err := manager.Get("TestDB-0$1")

	if err != nil {
		t.Errorf("Failed to get document: %v", err)
	}

	newdocument := SampleDocument{}
	ndoc.Export(&newdocument)

	expected := "New Title"

	if newdocument.Title != expected {
		t.Errorf("Unexpected document title. Expected '%v' got '%v'", expected, newdocument.Title)
	}

}

func TestSearch(t *testing.T) {
	manager, err := gomenasai.Load("test_database")

	defer manager.Close()

	if err != nil {
		t.Error(err)
	}

	searchResult := manager.Search("")

	if searchResult.Count == 0 {
		t.Errorf("No document found, expected > 0\n")
	}
}

func TestDelete(t *testing.T) {
	manager, err := gomenasai.Load("test_database")

	defer manager.Close()

	if err != nil {
		t.Error(err)
	}

	err = manager.Delete("TestDB-0$1")

	if err != nil {
		t.Errorf("Failed to delete document: %v", err)
	}
}
