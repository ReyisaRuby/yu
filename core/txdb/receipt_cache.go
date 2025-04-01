package txdb

import (
	"container/list"
	"sync"

	. "github.com/yu-org/yu/core/types"
)

// LRUCache represents the thread-safe LRU cache
type LRUCache struct {
	capacity int
	cache    map[[32]byte]*list.Element
	list     *list.List
	mutex    sync.Mutex
}

// entry represents a key-value pair in the cache
type entry struct {
	key   [32]byte
	value *Receipt
}

// NewLRUCache creates a new LRU cache with the given capacity
func NewLRUCache(capacity int) *LRUCache {
	return &LRUCache{
		capacity: capacity,
		cache:    make(map[[32]byte]*list.Element),
		list:     list.New(),
	}
}

// Get retrieves a value from the cache
func (l *LRUCache) Get(key [32]byte) (*Receipt, bool) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if elem, ok := l.cache[key]; ok {
		// Move the accessed element to the front of the list
		l.list.MoveToFront(elem)
		return elem.Value.(*entry).value, true
	}
	return nil, false
}

// Put adds or updates a value in the cache
func (l *LRUCache) Put(key [32]byte, value *Receipt) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	// If the key already exists, update the value and move to front
	if elem, ok := l.cache[key]; ok {
		elem.Value.(*entry).value = value
		l.list.MoveToFront(elem)
		return
	}

	// If the cache is full, evict the least recently used item
	if len(l.cache) >= l.capacity {
		// Get the back element
		elem := l.list.Back()
		if elem != nil {
			// Remove from the map and list
			delete(l.cache, elem.Value.(*entry).key)
			l.list.Remove(elem)
		}
	}

	// Add new entry to the front of the list and to the map
	newEntry := &entry{key, value}
	elem := l.list.PushFront(newEntry)
	l.cache[key] = elem
}

// Remove deletes a key from the cache
func (l *LRUCache) Remove(key [32]byte) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if elem, ok := l.cache[key]; ok {
		delete(l.cache, key)
		l.list.Remove(elem)
	}
}

// Len returns the current number of items in the cache
func (l *LRUCache) Len() int {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return len(l.cache)
}
