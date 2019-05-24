package gomenasai

import (
	"context"
	"fmt"
	"log"
	"sort"

	"gitlab.com/nokusukun/go-menasai/chunk"
)

type GomenasaiSearchResult struct {
	Documents []*chunk.Document
	Manager   *Gomenasai
}

func (sr *GomenasaiSearchResult) ExportJSONArray() ([]byte, error) {
	arr := []interface{}{}
	for _, doc := range sr.Documents {
		i := new(interface{})
		json.Unmarshal(doc.Content, &i)
		arr = append(arr, i)
	}
	return json.Marshal(arr)
}

func (sr *GomenasaiSearchResult) Filter(query string) *GomenasaiSearchResult {
	fmt.Println("Loading Filter", query)
	query = fmt.Sprintf("%v", query)
	toreturn := []*chunk.Document{}
	eval, err := sr.Manager.EvalEngine.NewEvaluable(query)

	if err != nil {
		fmt.Println("Failed to load filter: ", err)
	}

	for _, doc := range sr.Documents {
		mapdoc := make(map[string]interface{})
		json.Unmarshal(doc.Content, &mapdoc)

		value, err := eval(context.Background(), map[string]interface{}{
			"doc": mapdoc,
		})

		if err != nil {
			continue
		}

		val, ok := value.(bool)
		if ok {
			if val {
				toreturn = append(toreturn, doc)
			}
		}
	}
	sr.Documents = toreturn
	return sr
}

func (sr *GomenasaiSearchResult) Sort(query string) *GomenasaiSearchResult {
	eval, err := sr.Manager.EvalEngine.NewEvaluable(query)

	if err != nil {
		log.Println("Failed to load filter: ", err)
	}
	sort.Slice(sr.Documents, func(x, y int) bool {
		value, err := eval(context.Background(), map[string]interface{}{
			"x": sr.Documents[x].ExportI(),
			"y": sr.Documents[y].ExportI(),
		})
		if err != nil {
			log.Println("Failed to evaluate:", err)
			return false
		}
		val, ok := value.(bool)
		if !ok {
			log.Println("Failed to evaluate: Value is not bool:", value)
			return false
		}
		return val
	})
	return sr
}

func (sr *GomenasaiSearchResult) Limit(start, count int) *GomenasaiSearchResult {
	end := start + count
	if end > len(sr.Documents) {
		end = len(sr.Documents)
	}
	sr.Documents = sr.Documents[start:end]
	return sr
}
