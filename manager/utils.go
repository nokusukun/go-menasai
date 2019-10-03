package gomenasai

func Byte2Document(barr []byte) Document {
	var d Document
	_ = json.Unmarshal(barr, &d)
	return d
}
