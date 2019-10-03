package gomenasai

import (
	"context"
	"fmt"
	"log"
	"sort"

	"github.com/qntfy/kazaam"
)

type GomenasaiSearchResult struct {
	Documents []Document
	Manager   *Gomenasai
	Count     int
}

func (sr *GomenasaiSearchResult) ExportJSONArray() ([]byte, error) {
	arr := []interface{}{}
	for _, doc := range sr.Documents {
		i := new(interface{})
		_ = json.Unmarshal(doc.Content, &i)
		arr = append(arr, i)
	}
	return json.Marshal(arr)
}

func (sr *GomenasaiSearchResult) Filter(query string) *GomenasaiSearchResult {
	log.Println("Loading Filter", query)
	query = fmt.Sprintf("%v", query)
	var toReturn []Document
	eval, err := sr.Manager.EvalEngine.NewEvaluable(query)

	if err != nil {
		log.Println("Failed to load filter: ", err)
	}

	for _, doc := range sr.Documents {
		mapdoc := make(map[string]interface{})
		_ = json.Unmarshal(doc.Content, &mapdoc)

		value, err := eval(context.Background(), map[string]interface{}{
			"doc": mapdoc,
		})

		if err != nil {
			continue
		}

		val, ok := value.(bool)
		if ok {
			if val {
				toReturn = append(toReturn, doc)
			}
		}
	}
	sr.Documents = toReturn
	sr.Count = len(toReturn)
	return sr
}

// Sort sorts the results with a specified gval expression.
// 		`Sort("x.price.amount < y.price.amount")`
// 	Objects will be assigned to variable x and y
func (sr *GomenasaiSearchResult) Sort(query string) *GomenasaiSearchResult {
	eval, err := sr.Manager.EvalEngine.NewEvaluable(query)

	if err != nil {
		log.Println("Failed to load filter: ", err)
	}

	if len(sr.Documents) == 0 {
		return sr
	}

	sort.Slice(sr.Documents, func(x, y int) bool {
		if y >= len(sr.Documents) {
			return false
		}
		value, err := eval(context.Background(), map[string]interface{}{
			"x": sr.Documents[x].ExportI(),
			"y": sr.Documents[y].ExportI(),
		})
		if err != nil {
			log.Println("failed to evaluate:", err)
			return false
		}
		val, ok := value.(bool)
		if !ok {
			log.Println("failed to evaluate: Value is not bool:", value)
			return false
		}
		return val
	})
	return sr
}

// Limit truncates the results based on where to start the truncation and the count.
func (sr *GomenasaiSearchResult) Limit(start, count int) *GomenasaiSearchResult {
	if start >= len(sr.Documents) {
		log.Println("failed to limit, passed start value exceeds the result count")
		return sr
	}
	end := start + count
	if end > len(sr.Documents) {
		end = len(sr.Documents)
	}
	sr.Documents = sr.Documents[start:end]
	return sr
}

// Transform modifies the documents based on a Kazaam spec.
func (sr *GomenasaiSearchResult) Transform(spec string) *GomenasaiSearchResult {
	kz, err := kazaam.NewKazaam(spec)
	if err != nil {
		log.Println("failed to load spec in transform: ", err, "\nspec: ", spec)
		return sr
	}
	for idx, doc := range sr.Documents {
		// Dereference from pointer to value
		newdoc := doc
		// Transform the content of the document
		cont, err := kz.TransformInPlace(newdoc.Content)
		if err != nil {
			log.Println("failed to transform document", err)
		}
		// Overwrite the content with the transformed content
		newdoc.Content = cont
		// Flag the document to as a reexport
		newdoc.Reexport()
		// Overwrite the pointer of the old document to the new one
		sr.Documents[idx] = newdoc
	}
	return sr
}
