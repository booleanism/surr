package pools

import (
	"sync"

	"github.com/booleanism/surr/internal/protocols"
)

var siblingNamePoolSize = 256

func GetSiblingNamePoolSize() int {
	mu.Lock()
	defer mu.Unlock()

	return siblingNamePoolSize
}

func SetSiblingNamePoolSize(s int) int {
	mu.Lock()
	defer mu.Unlock()

	siblingNamePoolSize = s
	return s
}

var SiblingNamePool = sync.Pool{
	New: func() any {
		mu.Lock()
		defer mu.Unlock()

		b := make([]protocols.SiblingName, 0, siblingNamePoolSize)
		return &b
	},
}

func SiblingNamePoolResetAndPut(b *[]protocols.SiblingName) {
	*b = (*b)[:0]
	SiblingNamePool.Put(b)
}
