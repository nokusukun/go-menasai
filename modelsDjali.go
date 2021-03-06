package main

type DjaliListing struct {
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
	ShipsTo            []string      `json:"shipsTo"`
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
