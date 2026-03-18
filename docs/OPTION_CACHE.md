# Option Data Cache

The option data cache reduces calls to the Massive API by caching contract and quote data at multiple layers.

## Redis Backing

- **TTL**: 7 days (`option_cache_ttl_seconds`, default 604,800)
- **Key prefix**: `bff:optcache`
- **Freshness tracking**: Each cached value has a companion key suffixed with `:ts` storing the Unix timestamp when the entry was written. This enables age checks and staleness detection.

## In-Memory LRU Caches

Each `MassiveOptionGateway` instance maintains per-symbol LRU caches:

- Contract cache: 2,000 entries
- Quote cache: 10,000 entries
- Snapshot cache: 5,000 entries

These sit in front of Redis. A global budget of **50,000 entries** across all gateway instances limits total in-memory usage. When the budget is exceeded, new entries are not cached until eviction frees space.

## Staleness Warning

Entries older than **3 days** (`_FRESHNESS_WARN_SECONDS` = 86,400 × 3) are flagged as stale in `check_freshness()`. This is used for health checks and monitoring dashboards; cached data is still served until TTL expiry.
