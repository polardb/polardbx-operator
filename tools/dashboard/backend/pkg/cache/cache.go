package cache

import (
	"sort"
	"sync"
	"time"
)

// Item represents a cached item with expiration
type Item struct {
	Value      interface{}
	Expiration int64
	CreatedAt  int64
}

// IsExpired checks if the item has expired
func (item Item) IsExpired() bool {
	if item.Expiration == 0 {
		return false
	}
	return time.Now().UnixNano() > item.Expiration
}

// Cache is a thread-safe in-memory cache with TTL support
type Cache struct {
	items             map[string]Item
	mu                sync.RWMutex
	defaultExpiration time.Duration
	cleanupInterval   time.Duration
	stopCleanup       chan bool
}

// Config holds cache configuration
type Config struct {
	DefaultExpiration time.Duration
	CleanupInterval   time.Duration
}

// DefaultConfig returns default cache configuration
func DefaultConfig() Config {
	return Config{
		DefaultExpiration: 30 * time.Second,
		CleanupInterval:   60 * time.Second,
	}
}

// New creates a new cache with the given configuration
func New(cfg Config) *Cache {
	c := &Cache{
		items:             make(map[string]Item),
		defaultExpiration: cfg.DefaultExpiration,
		cleanupInterval:   cfg.CleanupInterval,
		stopCleanup:       make(chan bool),
	}

	// Start cleanup goroutine
	go c.cleanupLoop()

	return c
}

// Set adds an item to the cache with the default expiration
func (c *Cache) Set(key string, value interface{}) {
	c.SetWithExpiration(key, value, c.defaultExpiration)
}

// SetWithExpiration adds an item to the cache with a specific expiration
func (c *Cache) SetWithExpiration(key string, value interface{}, expiration time.Duration) {
	var exp int64
	if expiration > 0 {
		exp = time.Now().Add(expiration).UnixNano()
	}
	now := time.Now().UnixNano()

	c.mu.Lock()
	c.items[key] = Item{
		Value:      value,
		Expiration: exp,
		CreatedAt:  now,
	}
	c.mu.Unlock()
}

// Get retrieves an item from the cache
func (c *Cache) Get(key string) (interface{}, bool) {
	c.mu.RLock()
	item, found := c.items[key]
	c.mu.RUnlock()

	if !found {
		return nil, false
	}

	if item.IsExpired() {
		c.Delete(key)
		return nil, false
	}

	return item.Value, true
}

// GetOrSet retrieves an item from cache or sets it using the provided function
func (c *Cache) GetOrSet(key string, fn func() (interface{}, error)) (interface{}, error) {
	if value, found := c.Get(key); found {
		return value, nil
	}

	value, err := fn()
	if err != nil {
		return nil, err
	}

	c.Set(key, value)
	return value, nil
}

// GetOrSetWithExpiration retrieves an item from cache or sets it with custom expiration
func (c *Cache) GetOrSetWithExpiration(key string, expiration time.Duration, fn func() (interface{}, error)) (interface{}, error) {
	if value, found := c.Get(key); found {
		return value, nil
	}

	value, err := fn()
	if err != nil {
		return nil, err
	}

	c.SetWithExpiration(key, value, expiration)
	return value, nil
}

// Delete removes an item from the cache
func (c *Cache) Delete(key string) {
	c.mu.Lock()
	delete(c.items, key)
	c.mu.Unlock()
}

// DeleteByPrefix removes all items with keys starting with the given prefix
func (c *Cache) DeleteByPrefix(prefix string) {
	c.mu.Lock()
	for key := range c.items {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			delete(c.items, key)
		}
	}
	c.mu.Unlock()
}

// CountByPrefix returns the number of (non-expired) items whose keys start with the given prefix.
func (c *Cache) CountByPrefix(prefix string) int {
	now := time.Now().UnixNano()
	c.mu.RLock()
	defer c.mu.RUnlock()

	count := 0
	for key, item := range c.items {
		if len(key) < len(prefix) || key[:len(prefix)] != prefix {
			continue
		}
		if item.Expiration > 0 && now > item.Expiration {
			continue
		}
		count++
	}
	return count
}

// DeleteOldestByPrefix deletes oldest (by insertion time) items with the given prefix to keep at most keep items.
// Expired items are treated as lowest priority but are not deleted here; call cleanupLoop/deleteExpired for that.
func (c *Cache) DeleteOldestByPrefix(prefix string, keep int) {
	if keep <= 0 {
		c.DeleteByPrefix(prefix)
		return
	}

	now := time.Now().UnixNano()
	type entry struct {
		key       string
		createdAt int64
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	var entries []entry
	for key, item := range c.items {
		if len(key) < len(prefix) || key[:len(prefix)] != prefix {
			continue
		}
		if item.Expiration > 0 && now > item.Expiration {
			continue
		}
		entries = append(entries, entry{key: key, createdAt: item.CreatedAt})
	}
	if len(entries) <= keep {
		return
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].createdAt < entries[j].createdAt
	})
	for i := 0; i < len(entries)-keep; i++ {
		delete(c.items, entries[i].key)
	}
}

// Clear removes all items from the cache
func (c *Cache) Clear() {
	c.mu.Lock()
	c.items = make(map[string]Item)
	c.mu.Unlock()
}

// Count returns the number of items in the cache
func (c *Cache) Count() int {
	c.mu.RLock()
	count := len(c.items)
	c.mu.RUnlock()
	return count
}

// cleanupLoop periodically removes expired items
func (c *Cache) cleanupLoop() {
	ticker := time.NewTicker(c.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.deleteExpired()
		case <-c.stopCleanup:
			return
		}
	}
}

// deleteExpired removes all expired items
func (c *Cache) deleteExpired() {
	now := time.Now().UnixNano()

	c.mu.Lock()
	for key, item := range c.items {
		if item.Expiration > 0 && now > item.Expiration {
			delete(c.items, key)
		}
	}
	c.mu.Unlock()
}

// Stop stops the cleanup goroutine
func (c *Cache) Stop() {
	c.stopCleanup <- true
}

// Stats returns cache statistics
func (c *Cache) Stats() map[string]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()

	expired := 0
	now := time.Now().UnixNano()
	for _, item := range c.items {
		if item.Expiration > 0 && now > item.Expiration {
			expired++
		}
	}

	return map[string]interface{}{
		"total":   len(c.items),
		"expired": expired,
		"active":  len(c.items) - expired,
	}
}
