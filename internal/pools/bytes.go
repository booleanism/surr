package pools

import (
	"sync"
)

var (
	bytesPoolSize = 1024
	mu            sync.Mutex
)

func GetBytesPoolSize() int {
	mu.Lock()
	defer mu.Unlock()

	return bytesPoolSize
}

func SetBytesPoolSize(s int) int {
	mu.Lock()
	defer mu.Unlock()

	bytesPoolSize = s
	return s
}

var BytesPool = sync.Pool{
	New: func() any {
		mu.Lock()
		defer mu.Unlock()

		b := make([]byte, 0, bytesPoolSize)
		return &b
	},
}

func BytesPoolResetAndPut(b *[]byte) {
	*b = (*b)[:0]
	BytesPool.Put(b)
}
