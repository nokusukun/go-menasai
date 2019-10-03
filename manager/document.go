package gomenasai

import (
	"fmt"
)

type Document struct {
	ID      string `json:"id"`
	Content []byte `json:"body"`

	mapping map[string]interface{}
}

// Export exports the Document to a specified interface, like json.Unmarshal
func (d *Document) Export(interf interface{}) error {
	return json.Unmarshal(d.Content, interf)
}

// ExportI returns the document as an interface{}, cast it using `result.(OriginalType)`
func (d *Document) ExportI() map[string]interface{} {
	if d.mapping == nil {
		d.mapping = make(map[string]interface{})

		err := json.Unmarshal(d.Content, &d.mapping)
		if err != nil {
			fmt.Println("ExportI: failed to unmarshal: ", err)
		}
	}
	return d.mapping
}

// Reexport marks the document as unmapped
func (d *Document) Reexport() {
	d.mapping = nil
}

// MarshalJSON is used by the internal marshaller.
func (d *Document) MarshalJSON() ([]byte, error) {
	return json.Marshal(Document{
		d.ID,
		d.Content,
		nil,
	})
}
