package pools

import (
	"sync"

	"github.com/google/uuid"
)

var uuidPoolSize = 256

func GetUUIDPoolSize() int {
	mu.Lock()
	defer mu.Unlock()

	return uuidPoolSize
}

func SetUUIDPoolSize(s int) int {
	mu.Lock()
	defer mu.Unlock()

	uuidPoolSize = s
	return s
}

// UUIDPool holds reusable []uuid.UUID buffers for the calls that need a set of
// ids -- the live-node list behind the availability endpoint is one per request.
var UUIDPool = sync.Pool{
	New: func() any {
		mu.Lock()
		defer mu.Unlock()

		b := make([]uuid.UUID, 0, uuidPoolSize)
		return &b
	},
}

func UUIDPoolResetAndPut(b *[]uuid.UUID) {
	*b = (*b)[:0]
	UUIDPool.Put(b)
}
