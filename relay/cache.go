package relay

import (
	"time"

	cache "github.com/patrickmn/go-cache"
)

// RelayCache is the in-mem cache used to store BeringeiPoint
var RelayCache *cache.Cache

func init() {
	RelayCache = cache.New(5*time.Minute, 10*time.Minute)
}
