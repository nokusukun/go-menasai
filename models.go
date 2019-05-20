package main

type JSONResponse struct {
	Metadata Metadata `json:"metadata"`
	Results  []Result `json:"results"`
}

type Metadata struct {
	ResponseInfo  ResponseInfo `json:"responseInfo"`
	Resultset     Resultset    `json:"resultset"`
	ExecutionTime float64      `json:"executionTime"`
}

type ResponseInfo struct {
	Status           int64  `json:"status"`
	DeveloperMessage string `json:"developerMessage"`
}

type Resultset struct {
	Count    int64 `json:"count"`
	Pagesize int64 `json:"pagesize"`
	Page     int64 `json:"page"`
}

type Result struct {
	Attachments []interface{} `json:"attachments"`
	Body        string        `json:"body"`
	Changed     string        `json:"changed"`
	Component   []Component   `json:"component"`
	Created     string        `json:"created"`
	Date        string        `json:"date"`
	Image       []interface{} `json:"image"`
	Teaser      []interface{} `json:"teaser"`
	Title       string        `json:"title"`
	Topic       []interface{} `json:"topic"`
	URL         string        `json:"url"`
	UUID        string        `json:"uuid"`
	Vuuid       string        `json:"vuuid"`
}

type Component struct {
	UUID string `json:"uuid"`
	Name string `json:"name"`
}
