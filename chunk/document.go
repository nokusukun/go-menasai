package chunk

type Document struct {
	ID      string `json:"id"`
	Content []byte `json:"body"`

	mapping map[string]interface{}
}

// Export exports the Document to a specified interface, like json.Unmarshal
func (d *Document) Export(interf interface{}) {
	json.Unmarshal(d.Content, interf)
}

// ExportI returns the document as an interface{}, cast it using `result.(OriginalType)`
func (d *Document) ExportI() map[string]interface{} {
	if d.mapping == nil {
		d.mapping = make(map[string]interface{})
		json.Unmarshal(d.Content, &d.mapping)
	}
	return d.mapping
}

// MarshalJSON is used by the internal marshaller.
func (d *Document) MarshalJSON() ([]byte, error) {
	return json.Marshal(Document{
		d.ID,
		d.Content,
		nil,
	})
}
