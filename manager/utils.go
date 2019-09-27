package gomenasai

import (
	"github.com/nokusukun/go-menasai/chunk"
)

func Byte2Document(barr []byte) chunk.Document {
	var d chunk.Document
	_ = json.Unmarshal(barr, &d)
	return d
}
