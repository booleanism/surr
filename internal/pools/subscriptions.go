package pools

import (
	"sync"

	"github.com/booleanism/surr/internal/models"
)

var subscriptionsPoolSize = 256

func GetSubscriptionsPoolSize() int {
	mu.Lock()
	defer mu.Unlock()

	return subscriptionsPoolSize
}

func SetSubscriptionsPoolSize(s int) int {
	mu.Lock()
	defer mu.Unlock()

	subscriptionsPoolSize = s
	return s
}

var SubscriptionsPool = sync.Pool{
	New: func() any {
		mu.Lock()
		defer mu.Unlock()

		b := make([]models.Subscription, 0, subscriptionsPoolSize)
		return &b
	},
}

func SubscriptionPoolResetAndPut(b *[]models.Subscription) {
	*b = (*b)[:0]
	SubscriptionsPool.Put(b)
}

var subscribedNodesPoolSize = 256

func GetSubscribedNodesPoolSize() int {
	mu.Lock()
	defer mu.Unlock()

	return subscribedNodesPoolSize
}

func SetSubscribedNodesPoolSize(s int) int {
	mu.Lock()
	defer mu.Unlock()

	subscribedNodesPoolSize = s
	return s
}

var SubscribedNodesPool = sync.Pool{
	New: func() any {
		mu.Lock()
		defer mu.Unlock()

		b := make([]models.SubscribedNodes, 0, subscribedNodesPoolSize)
		return &b
	},
}

func SubscribedNodesPoolResetAndPut(b *[]models.SubscribedNodes) {
	*b = (*b)[:0]
	SubscribedNodesPool.Put(b)
}
