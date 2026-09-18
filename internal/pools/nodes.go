package pools

import (
	"sync"

	"github.com/booleanism/surr/internal/models"
)

var nodePoolSize = 256

func GetNodePoolSize() int {
	mu.Lock()
	defer mu.Unlock()

	return nodePoolSize
}

func SetNodePoolSize(s int) int {
	mu.Lock()
	defer mu.Unlock()

	nodePoolSize = s
	return s
}

var NodePool = sync.Pool{
	New: func() any {
		mu.Lock()
		defer mu.Unlock()

		b := make([]models.Node, 0, nodePoolSize)
		return &b
	},
}

func NodePoolResetAndPut(b *[]models.Node) {
	*b = (*b)[:0]
	NodePool.Put(b)
}
