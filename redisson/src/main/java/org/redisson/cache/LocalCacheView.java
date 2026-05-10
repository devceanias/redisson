/**
 * Copyright (c) 2013-2026 Nikita Koksharov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.redisson.cache;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.MonthDay;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.UUID;

import com.github.benmanes.caffeine.cache.Caffeine;
import org.redisson.RedissonObject;
import org.redisson.api.LocalCachedMapOptions;
import org.redisson.misc.Hash;

import io.netty.buffer.ByteBuf;

/**
 *
 * @author Nikita Koksharov
 *
 * @param <K> key type
 * @param <V> value type
 */
public class LocalCacheView<K, V> {

    private final RedissonObject object;
    private final ConcurrentMap<Object, CacheValue> cache;
    private final ConcurrentMap<CacheKey, Object> cacheKeyMap;
    private final boolean useObjectAsCacheKey;

    public LocalCacheView(LocalCachedMapOptions<?, ?> options, RedissonObject object) {
        this.cache = createCache(options);
        this.object = object;
        this.cacheKeyMap = new ConcurrentHashMap<>();
        this.useObjectAsCacheKey = options.isUseObjectAsCacheKey();
    }

    public Set<K> cachedKeySet() {
        return new LocalKeySet();
    }

    class LocalKeySet extends AbstractSet<K> {

        @Override
        public Iterator<K> iterator() {
            return new Iterator<K>() {

                private Iterator<CacheValue> iter = cache.values().iterator();

                @Override
                public boolean hasNext() {
                    return iter.hasNext();
                }

                @Override
                public K next() {
                    return (K) iter.next().getKey();
                }

                @Override
                public void remove() {
                    removeCacheKey(((AbstractCacheMap.MapIterator) iter).cursorValue().getKey());
                    iter.remove();
                }
            };
        }

        @Override
        public boolean contains(Object o) {
            return cache.containsKey(toLookupKey(o));
        }

        @Override
        public boolean remove(Object o) {
            final CacheValue value = cache.remove(toLookupKey(o));

            if (value == null) {
                return false;
            }

            removeCacheKey(value.getKey());

            return true;
        }

        @Override
        public int size() {
            return cache.size();
        }

        @Override
        public void clear() {
            cache.clear();
            cacheKeyMap.clear();
        }

    }

    public Collection<V> cachedValues() {
        return new LocalValues();
    }

    final class LocalValues extends AbstractCollection<V> {

        @Override
        public Iterator<V> iterator() {
            return new Iterator<V>() {

                private Iterator<CacheValue> iter = cache.values().iterator();

                @Override
                public boolean hasNext() {
                    return iter.hasNext();
                }

                @Override
                public V next() {
                    return (V) iter.next().getValue();
                }

                @Override
                public void remove() {
                    removeCacheKey(((AbstractCacheMap.MapIterator) iter).cursorValue().getKey());
                    iter.remove();
                }
            };
        }

        @Override
        public boolean contains(Object o) {
            CacheValue cacheValue = new CacheValue(null, o);
            return cache.containsValue(cacheValue);
        }

        @Override
        public int size() {
            return cache.size();
        }

        @Override
        public void clear() {
            cache.clear();
            cacheKeyMap.clear();
        }

    }

    public Set<Entry<K, V>> cachedEntrySet() {
        return new LocalEntrySet();
    }

    final class LocalEntrySet extends AbstractSet<Map.Entry<K, V>> {

        @Override
        public Iterator<Map.Entry<K, V>> iterator() {
            return new Iterator<Map.Entry<K, V>>() {

                private Iterator<CacheValue> iter = cache.values().iterator();

                @Override
                public boolean hasNext() {
                    return iter.hasNext();
                }

                @Override
                public Map.Entry<K, V> next() {
                    CacheValue e = iter.next();
                    V val = toValue(e);
                    return new AbstractMap.SimpleEntry<K, V>((K) e.getKey(), val);
                }

                @Override
                public void remove() {
                    removeCacheKey(((AbstractCacheMap.MapIterator) iter).cursorValue().getKey());
                    iter.remove();
                }
            };
        }

        @Override
        public boolean contains(Object o) {
            if (!(o instanceof Map.Entry))
                return false;
            Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;

            final CacheValue entry = cache.get(toLookupKey(e.getKey()));

            return entry != null && entry.getValue().equals(e.getValue());
        }

        @Override
        public boolean remove(Object o) {
            if (o instanceof Map.Entry) {
                Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;

                final CacheValue value = cache.remove(toLookupKey(e.getKey()));

                if (value == null) {
                    return false;
                }

                removeCacheKey(value.getKey());

                return true;
            }
            return false;
        }

        @Override
        public int size() {
            return cache.size();
        }

        @Override
        public void clear() {
            cache.clear();
            cacheKeyMap.clear();
        }

    }

    public Map<K, V> getCachedMap() {
        return new LocalMap();
    }

    final class LocalMap extends AbstractMap<K, V> {

        @Override
        public V get(Object key) {
            CacheValue e = cache.get(toLookupKey(key));
            if (e != null) {
                return (V) e.getValue();
            }
            return null;
        }

        @Override
        public boolean containsKey(Object key) {
            return cache.containsKey(toLookupKey(key));
        }

        @Override
        public boolean containsValue(Object value) {
            CacheValue cacheValue = new CacheValue(null, value);
            return cache.containsValue(cacheValue);
        }

        @Override
        public Set<Entry<K, V>> entrySet() {
            return cachedEntrySet();
        }

    }

    protected V toValue(CacheValue cv) {
        return (V) cv.getValue();
    }

    public CacheKey toCacheKey(Object key) {
        ByteBuf encoded = encodeMapKey(key);
        try {
            return toCacheKey(encoded);
        } finally {
            encoded.release();
        }
    }

    protected ByteBuf encodeMapKey(Object key) {
        return object.encodeMapKey(key);
    }

    public Object toLookupKey(Object key) {
        if (isObjectLookupKey(key)) {
            return key;
        }

        return toCacheKey(key);
    }

    public Object toLookupKey(final CacheKey cacheKey, final Object key) {
        if (!isObjectLookupKey(key)) {
            return cacheKey;
        }

        cacheKeyMap.put(cacheKey, key);

        return key;
    }

    public void removeCacheKey(final Object key) {
        if (!isObjectLookupKey(key)) {
            return;
        }

        cacheKeyMap.entrySet().removeIf(entry -> Objects.equals(entry.getValue(), key));
    }

    public Object removeCacheKey(final CacheKey cacheKey) {
        return cacheKeyMap.remove(cacheKey);
    }

    public CacheKey toCacheKey(ByteBuf encodedKey) {
        return new CacheKey(Hash.hash128toArray(encodedKey));
    }

    public <K1, V1> ConcurrentMap<K1, V1> getCache() {
        return (ConcurrentMap<K1, V1>) cache;
    }

    public ConcurrentMap<CacheKey, Object> getCacheKeyMap() {
        return cacheKeyMap;
    }

    private boolean isObjectLookupKey(final Object key) {
        if (key == null) {
            return false;
        }

        return useObjectAsCacheKey
            || key instanceof String
            || key instanceof UUID
            || key instanceof Enum
            || key instanceof Boolean
            || key instanceof Byte
            || key instanceof Short
            || key instanceof Integer
            || key instanceof Long
            || key instanceof Float
            || key instanceof Double
            || key instanceof Character
            || key instanceof BigInteger
            || key instanceof BigDecimal
            || key instanceof Instant
            || key instanceof LocalDate
            || key instanceof LocalDateTime
            || key instanceof LocalTime
            || key instanceof OffsetDateTime
            || key instanceof OffsetTime
            || key instanceof ZonedDateTime
            || key instanceof Year
            || key instanceof YearMonth
            || key instanceof MonthDay
            || key instanceof Duration
            || key instanceof Period
            || key instanceof Class
            || key.getClass().isRecord();
    }

    public <K1, V1> ConcurrentMap<K1, V1> createCache(LocalCachedMapOptions<?, ?> options) {
        if (options.getCacheSize() == -1) {
            return new NoOpCacheMap<>();
        }

        if (options.getCacheProvider() == LocalCachedMapOptions.CacheProvider.CAFFEINE) {
            Caffeine<Object, Object> caffeineBuilder = Caffeine.newBuilder();
            if (options.getTimeToLiveInMillis() > 0) {
                caffeineBuilder.expireAfterWrite(options.getTimeToLiveInMillis(), TimeUnit.MILLISECONDS);
            }
            if (options.getMaxIdleInMillis() > 0) {
                caffeineBuilder.expireAfterAccess(options.getMaxIdleInMillis(), TimeUnit.MILLISECONDS);
            }
            if (options.getCacheSize() > 0) {
                caffeineBuilder.maximumSize(options.getCacheSize());
            }
            if (options.getEvictionPolicy() == LocalCachedMapOptions.EvictionPolicy.SOFT) {
                caffeineBuilder.softValues();
            }
            if (options.getEvictionPolicy() == LocalCachedMapOptions.EvictionPolicy.WEAK) {
                caffeineBuilder.weakValues();
            }
            return caffeineBuilder.<K1, V1>build().asMap();
        }

        if (options.getEvictionPolicy() == LocalCachedMapOptions.EvictionPolicy.NONE) {
            return new NoneCacheMap<>(options.getTimeToLiveInMillis(), options.getMaxIdleInMillis());
        }
        if (options.getEvictionPolicy() == LocalCachedMapOptions.EvictionPolicy.LRU) {
            return new LRUCacheMap<>(options.getCacheSize(), options.getTimeToLiveInMillis(), options.getMaxIdleInMillis());
        }
        if (options.getEvictionPolicy() == LocalCachedMapOptions.EvictionPolicy.LFU) {
            return new LFUCacheMap<>(options.getCacheSize(), options.getTimeToLiveInMillis(), options.getMaxIdleInMillis());
        }
        if (options.getEvictionPolicy() == LocalCachedMapOptions.EvictionPolicy.SOFT) {
            return ReferenceCacheMap.soft(options.getTimeToLiveInMillis(), options.getMaxIdleInMillis());
        }
        if (options.getEvictionPolicy() == LocalCachedMapOptions.EvictionPolicy.WEAK) {
            return ReferenceCacheMap.weak(options.getTimeToLiveInMillis(), options.getMaxIdleInMillis());
        }
        throw new IllegalArgumentException("Invalid eviction policy: " + options.getEvictionPolicy());
    }


}