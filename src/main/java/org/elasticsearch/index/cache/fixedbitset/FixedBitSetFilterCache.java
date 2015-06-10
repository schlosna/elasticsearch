/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.cache.fixedbitset;

import com.google.common.base.MoreObjects;
import com.google.common.base.Stopwatch;
import com.google.common.cache.*;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.component.CloseableComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.lucene.SegmentReaderUtils;
import org.elasticsearch.common.lucene.search.NoCacheFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.search.nested.NonNestedDocsFilter;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.service.InternalIndexService;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardUtils;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.warmer.IndicesWarmer;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.*;

/**
 * This is a cache for {@link FixedBitSet} based filters and is unbounded by size or time.
 * <p/>
 * Use this cache with care, only components that require that a filter is to be materialized as a {@link FixedBitSet}
 * and require that it should always be around should use this cache, otherwise the
 * {@link org.elasticsearch.index.cache.filter.FilterCache} should be used instead.
 */
public class FixedBitSetFilterCache extends AbstractIndexComponent implements AtomicReader.CoreClosedListener, RemovalListener<Object, Cache<Filter, FixedBitSetFilterCache.Value>>, CloseableComponent {

    public static final String LOAD_RANDOM_ACCESS_FILTERS_EAGERLY = "index.load_fixed_bitset_filters_eagerly";
    public static final String CACHE_CONCURRENCY_LEVEL = "index.cache.fixedbitset.concurrency_level";
    public static final String CLEAN_INTERVAL_SETTING = "index.cache.fixedbitset.cleanup_interval";

    private static final int VALUE_BASE_RAM_BYTES_USED = (int) RamUsageEstimator.shallowSizeOfInstance(Value.class);

    private final boolean loadRandomAccessFiltersEagerly;
    private final Cache<Object, Cache<Filter, Value>> loadedFilters;
    private final FixedBitSetFilterWarmer warmer;
    private final long sizeInBytes;
    private final TimeValue expire;
    private final ThreadPool threadPool;
    private final TimeValue cleanInterval;
    private volatile boolean closed = false;

    private IndexService indexService;
    private IndicesWarmer indicesWarmer;

    @Inject
    public FixedBitSetFilterCache(Index index, @IndexSettings Settings indexSettings, ThreadPool threadPool) {
        super(index, indexSettings);
        this.loadRandomAccessFiltersEagerly = indexSettings.getAsBoolean(LOAD_RANDOM_ACCESS_FILTERS_EAGERLY, true);

        String size = componentSettings.get("size", "-1");
        sizeInBytes = componentSettings.getAsMemory("size", "-1").bytes();
        expire = componentSettings.getAsTime("expire", null);

        CacheBuilder<Object, Cache<Filter, Value>> cacheBuilder = CacheBuilder.newBuilder().removalListener(this);

        if (sizeInBytes > 0) {
            cacheBuilder.maximumWeight(toKiB(sizeInBytes))
                    .weigher(new FixedBitSetFilterCacheWeigher());
        }
        final int concurrencyLevel =  indexSettings.getAsInt(CACHE_CONCURRENCY_LEVEL, 4);
        if (concurrencyLevel <= 0) {
            throw new ElasticsearchIllegalArgumentException("concurrency_level must be > 0 but was: " + concurrencyLevel);
        }
        cacheBuilder.concurrencyLevel(concurrencyLevel);
        if (expire != null && expire.millis() > 0) {
            cacheBuilder.expireAfterAccess(expire.millis(), TimeUnit.MILLISECONDS);
        }
        logger.debug("using size [{}] [{}], expire [{}]", size, new ByteSizeValue(sizeInBytes), expire);

        if (logger.isDebugEnabled()) {
            cacheBuilder.recordStats();
        }

        this.loadedFilters = cacheBuilder.build();
        this.warmer = new FixedBitSetFilterWarmer();
        this.threadPool = threadPool;
        this.cleanInterval = indexSettings.getAsTime(CLEAN_INTERVAL_SETTING, TimeValue.timeValueMinutes(1));
        // Start thread that will manage cleaning the filter cache periodically
        threadPool.schedule(this.cleanInterval, ThreadPool.Names.SAME,
                new CacheCleaner(this.loadedFilters, this.logger, this.threadPool, this.cleanInterval));
    }

    static class FixedBitSetFilterCacheWeigher implements Weigher<Object, Cache<Filter, Value>> {
        @Override
        public int weigh(Object key, Cache<Filter, Value> cache) {
            long bytes = RamUsageEstimator.shallowSizeOf(key);
            if (cache != null) {
                bytes += RamUsageEstimator.shallowSizeOf(cache);
                for (Entry<Filter, Value> entry : cache.asMap().entrySet()) {
                    bytes += ramBytesUsed(entry.getKey(), entry.getValue());
                }
            }
            return toKiB(bytes);
        }
    }

    static class FixedBitSetFilterWeigher implements Weigher<Filter, Value> {
        @Override
        public int weigh(Filter filter, Value value) {
            return toKiB(ramBytesUsed(filter, value));
        }
    }

    static long ramBytesUsed(Filter filter, Value value) {
        return ramBytesUsed(filter) + ramBytesUsed(value);
    }

    static long ramBytesUsed(Filter filter) {
        long bytes = RamUsageEstimator.shallowSizeOf(filter);
        if (filter instanceof Accountable) {
            bytes += ((Accountable) filter).ramBytesUsed();
        }
        return bytes;
    }

    static long ramBytesUsed(Value value) {
        if (value == null || value.fixedBitSet == null) {
            return VALUE_BASE_RAM_BYTES_USED;
        }
        return RamUsageEstimator.shallowSizeOf(value) + value.fixedBitSet.ramBytesUsed();
    }

    static int toKiB(long bytes) {
        return toBoundedInt(bytes / 1024);
    }

    static int toBoundedInt(long n) {
        return (n > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int)n;
    }

    @Inject(optional = true)
    public void setIndicesWarmer(IndicesWarmer indicesWarmer) {
        this.indicesWarmer = indicesWarmer;
    }

    public void setIndexService(InternalIndexService indexService) {
        this.indexService = indexService;
        // First the indicesWarmer is set and then the indexService is set, because of this there is a small window of
        // time where indexService is null. This is why the warmer should only registered after indexService has been set.
        // Otherwise there is a small chance of the warmer running into a NPE, since it uses the indexService
        indicesWarmer.addListener(warmer);
    }

    public FixedBitSetFilter getFixedBitSetFilter(Filter filter) {
        assert filter != null;
        assert !(filter instanceof NoCacheFilter);
        return new FixedBitSetFilterWrapper(filter);
    }

    @Override
    public void onClose(Object ownerCoreCacheKey) {
        loadedFilters.invalidate(ownerCoreCacheKey);
        logger.debug("Invalidated cache entry for closed key [{}]", ownerCoreCacheKey);
        logCacheStats();
    }

    public void close() throws ElasticsearchException {
        indicesWarmer.removeListener(warmer);
        clear("close");
        closed = true;
    }

    public void clear(String reason) {
        logger.debug("Clearing all FixedBitSets because [{}]", reason);
        loadedFilters.invalidateAll();
        loadedFilters.cleanUp();
    }

    private FixedBitSet getAndLoadIfNotPresent(final Filter filter, final AtomicReaderContext context) throws IOException, ExecutionException {
        final Object coreCacheReader = context.reader().getCoreCacheKey();
        final ShardId shardId = ShardUtils.extractShardId(context.reader());
        Cache<Filter, Value> filterToFbs = loadedFilters.get(coreCacheReader, createFilterValueLoader(context));
        return filterToFbs.get(filter, createFixedBitSetFilterLoader(filter, context, shardId)).fixedBitSet;
    }

    private Callable<Value> createFixedBitSetFilterLoader(final Filter filter, final AtomicReaderContext context, final ShardId shardId) {
        return new Callable<Value>() {
            @Override
            public Value call() throws Exception {
                DocIdSet docIdSet = filter.getDocIdSet(context, null);
                final FixedBitSet fixedBitSet;
                if (docIdSet instanceof FixedBitSet) {
                    fixedBitSet = (FixedBitSet) docIdSet;
                } else {
                    Stopwatch timer = Stopwatch.createStarted();
                    fixedBitSet = new FixedBitSet(context.reader().maxDoc());
                    if (docIdSet != null && docIdSet != DocIdSet.EMPTY) {
                        DocIdSetIterator iterator = docIdSet.iterator();
                        if (iterator != null) {
                            int doc = iterator.nextDoc();
                            if (doc != DocIdSetIterator.NO_MORE_DOCS) {
                                do {
                                    fixedBitSet.set(doc);
                                    doc = iterator.nextDoc();
                                } while (doc != DocIdSetIterator.NO_MORE_DOCS);
                            }
                        }
                    }

                    if (logger.isDebugEnabled()) {
                        logger.debug("Generated [{}] FixedBitSet for reader key [{}], filter [{}] of [{}] docs in [{}ms]",
                                new ByteSizeValue(fixedBitSet.ramBytesUsed()),
                                context.reader().getCoreCacheKey(),
                                filter,
                                fixedBitSet.cardinality(),
                                timer.elapsed(TimeUnit.MILLISECONDS));
                    }
                    logger.debug("Generated fixedBitSet from DocIdSet {} in {}", docIdSet, timer);
                }

                Value value = new Value(fixedBitSet, shardId);
                onCached(filter, value);

                return value;
            }
        };
    }

    private Callable<Cache<Filter, Value>> createFilterValueLoader(final AtomicReaderContext context) {
        return new Callable<Cache<Filter, Value>>() {
            @Override
            public Cache<Filter, Value> call() throws Exception {
                SegmentReaderUtils.registerCoreListener(context.reader(), FixedBitSetFilterCache.this);

                CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
                if (sizeInBytes > 0) {
                    cacheBuilder.maximumWeight(sizeInBytes).weigher(new FixedBitSetFilterWeigher());
                }
                if (expire != null && expire.millis() > 0) {
                    cacheBuilder.expireAfterAccess(expire.millis(), TimeUnit.MILLISECONDS);
                }
                cacheBuilder.removalListener(new RemovalListener<Filter, Value>() {
                    @Override
                    public void onRemoval(RemovalNotification<Filter, Value> notification) {
                        if (notification.getKey() == null || notification.getValue() == null) {
                            return;
                        }
                        FixedBitSetFilterCache.this.onRemoval(notification.getKey(), notification.getValue(), notification.getCause());
                    }

                });

                if (logger.isDebugEnabled()) {
                    cacheBuilder.recordStats();
                }

                Cache<Filter, Value> cache = cacheBuilder.build();
                logger.debug("Created cache for key: [{}]",
                        context.reader().getCoreCacheKey());
                return cache;
            }
        };
    }

    void onCached(Filter filter, Value value) {
        logger.debug("Added FixedBitSetFilterCache entry for filter: [{}], value: [{}]",
                filter, value);
        if (value.shardId != null) {
            IndexShard shard = indexService.shard(value.shardId.id());
            if (shard != null) {
                shard.shardFixedBitSetFilterCache().onCached(value.fixedBitSet.ramBytesUsed());
                if (logger.isDebugEnabled()) {
                    logger.debug("Shard {} FixedBitSetFilterCache size: [{}]",
                            value.shardId,
                            new ByteSizeValue(shard.shardFixedBitSetFilterCache().getMemorySizeInBytes()));
                }
            }
        }

        logCacheStats();
    }

    void onRemoval(Filter filter, Value value, RemovalCause cause) {
        logger.debug("Removed FixedBitSetFilterCache entry for filter: [{}], value: [{}], cause: [{}]",
                filter, value, cause);
        if (value.shardId != null) {
            IndexShard shard = indexService.shard(value.shardId.id());
            if (shard != null) {
                shard.shardFixedBitSetFilterCache().onRemoval(value.fixedBitSet.ramBytesUsed());
                if (logger.isDebugEnabled()) {
                    logger.debug("Shard {} FixedBitSetFilterCache size: [{}]",
                            value.shardId,
                            new ByteSizeValue(shard.shardFixedBitSetFilterCache().getMemorySizeInBytes()));
                }
            }
            // if null then this means the shard has already been removed and the stats are 0 anyway
            // for the shard this key belongs to
        }

        logCacheStats();
    }

    void logCacheStats() {
        if (logger.isDebugEnabled()) {
            CacheStats stats = getLoadedFilters().stats();
            long totalBytes = 0;
            for (Entry<Object, Cache<Filter, Value>> caches : getLoadedFilters().asMap().entrySet()) {
                Cache<Filter, Value> filterCache = caches.getValue();
                long cacheBytes = 0;
                for (Entry<Filter, Value> entry : filterCache.asMap().entrySet()) {
                    long entryBytes = ramBytesUsed(entry.getKey(), entry.getValue());
                    cacheBytes += entryBytes;
                    if (logger.isTraceEnabled()) {
                        logger.trace("Entry key [{}] for filter [{}]: [{}]",
                                caches.getKey(),
                                entry.getKey(),
                                new ByteSizeValue(entryBytes));
                    }
                }
                totalBytes += cacheBytes;
                logCacheDetails("Filter cache for key [" + caches.getKey() + "]", filterCache.stats(), cacheBytes);
            }
            logCacheDetails("FixedBitSetFilterCache", stats, totalBytes);
        }
    }

    private void logCacheDetails(String name, CacheStats stats, long bytes) {
        logger.debug("{} stats {}", name,
                MoreObjects.toStringHelper("")
                        .add("size", getLoadedFilters().size())
                        .add("memory", new ByteSizeValue(bytes))
                        .add("hitRate", stats.hitRate())
                        .add("hitCount", stats.hitCount())
                        .add("missCount", stats.missCount())
                        .add("loadCount", stats.loadCount())
                        .add("evictions", stats.evictionCount())
                        .add("avgLoadPenaltyMs", TimeUnit.MILLISECONDS.convert((long) stats.averageLoadPenalty(), TimeUnit.NANOSECONDS))
                        .add("totalLoadTimeMs", TimeUnit.MILLISECONDS.convert(stats.totalLoadTime(), TimeUnit.NANOSECONDS))
                        .add("exceptionCount", stats.loadExceptionCount())
                        .toString());
    }

    @Override
    public void onRemoval(RemovalNotification<Object, Cache<Filter, Value>> notification) {
        Object key = notification.getKey();
        if (key == null) {
            return;
        }

        Cache<Filter, Value> value = notification.getValue();
        if (value == null) {
            return;
        }

        for (Map.Entry<Filter, Value> entry : value.asMap().entrySet()) {
            onRemoval(entry.getKey(), entry.getValue(), notification.getCause());
        }
    }

    public static final class Value {

        final FixedBitSet fixedBitSet;
        final ShardId shardId;

        public Value(FixedBitSet fixedBitSet, ShardId shardId) {
            this.fixedBitSet = fixedBitSet;
            this.shardId = shardId;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass())
                    .add("shardId", shardId)
                    .add("fixedBitSetDocs", fixedBitSet.cardinality())
                    .add("fixedBitSetSize", new ByteSizeValue(fixedBitSet.ramBytesUsed()))
                    .toString();
        }
    }

    final class FixedBitSetFilterWrapper extends FixedBitSetFilter {

        final Filter filter;

        FixedBitSetFilterWrapper(Filter filter) {
            this.filter = filter;
        }

        @Override
        public FixedBitSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
            try {
                return getAndLoadIfNotPresent(filter, context);
            } catch (ExecutionException e) {
                throw ExceptionsHelper.convertToElastic(e);
            }
        }

        public String toString() {
            return "random_access(" + filter + ")";
        }

        public boolean equals(Object o) {
            if (!(o instanceof FixedBitSetFilterWrapper)) return false;
            return this.filter.equals(((FixedBitSetFilterWrapper) o).filter);
        }

        public int hashCode() {
            return filter.hashCode() ^ 0x1117BF26;
        }
    }

    final class FixedBitSetFilterWarmer extends IndicesWarmer.Listener {

        @Override
        public TerminationHandle warmNewReaders(final IndexShard indexShard, IndexMetaData indexMetaData, IndicesWarmer.WarmerContext context, ThreadPool threadPool) {
            if (!loadRandomAccessFiltersEagerly) {
                return TerminationHandle.NO_WAIT;
            }

            boolean hasNested = false;
            final Set<Filter> warmUp = new HashSet<>();
            final MapperService mapperService = indexShard.mapperService();
            for (DocumentMapper docMapper : mapperService.docMappers(false)) {
                if (docMapper.hasNestedObjects()) {
                    hasNested = true;
                    for (ObjectMapper objectMapper : docMapper.objectMappers().values()) {
                        if (objectMapper.nested().isNested()) {
                            ObjectMapper parentObjectMapper = docMapper.findParentObjectMapper(objectMapper);
                            if (parentObjectMapper != null && parentObjectMapper.nested().isNested()) {
                                warmUp.add(parentObjectMapper.nestedTypeFilter());
                            }
                        }
                    }
                }
            }

            if (hasNested) {
                warmUp.add(NonNestedDocsFilter.INSTANCE);
            }

            final Executor executor = threadPool.executor(executor());
            final CountDownLatch latch = new CountDownLatch(context.searcher().reader().leaves().size() * warmUp.size());
            for (final AtomicReaderContext ctx : context.searcher().reader().leaves()) {
                for (final Filter filterToWarm : warmUp) {
                    executor.execute(new Runnable() {

                        @Override
                        public void run() {
                            try {
                                final long start = System.nanoTime();
                                getAndLoadIfNotPresent(filterToWarm, ctx);
                                if (indexShard.warmerService().logger().isTraceEnabled()) {
                                    indexShard.warmerService().logger().trace("warmed fixed bitset for [{}], took [{}]", filterToWarm, TimeValue.timeValueNanos(System.nanoTime() - start));
                                }
                            } catch (Throwable t) {
                                indexShard.warmerService().logger().warn("failed to load fixed bitset for [{}]", t, filterToWarm);
                            } finally {
                                latch.countDown();
                            }
                        }

                    });
                }
            }
            return new TerminationHandle() {
                @Override
                public void awaitTermination() throws InterruptedException {
                    latch.await();
                }
            };
        }

        @Override
        public TerminationHandle warmTopReader(IndexShard indexShard, IndexMetaData indexMetaData, IndicesWarmer.WarmerContext context, ThreadPool threadPool) {
            return TerminationHandle.NO_WAIT;
        }

    }

    Cache<Object, Cache<Filter, Value>> getLoadedFilters() {
        return loadedFilters;
    }

    /**
     * CacheCleaner is a scheduled Runnable used to clean a Guava cache
     * periodically. In this case it is the field data cache, because a cache that
     * has an entry invalidated may not clean up the entry if it is not read from
     * or written to after invalidation.
     */
    public class CacheCleaner implements Runnable {

        private final Cache<?, ?> cache;
        private final ESLogger logger;
        private final ThreadPool threadPool;
        private final TimeValue interval;

        public CacheCleaner(Cache<?, ?> cache, ESLogger logger, ThreadPool threadPool, TimeValue interval) {
            this.cache = cache;
            this.logger = logger;
            this.threadPool = threadPool;
            this.interval = interval;
        }

        @Override
        public void run() {
            long startTime = System.currentTimeMillis();
            if (logger.isTraceEnabled()) {
                logger.trace("running periodic field data cache cleanup");
            }
            try {
                this.cache.cleanUp();
            } catch (Exception e) {
                logger.warn("Exception during periodic field data cache cleanup:", e);
            }
            if (logger.isTraceEnabled()) {
                logger.trace("periodic field data cache cleanup finished in {} milliseconds", System.currentTimeMillis() - startTime);
            }
            logCacheStats();
            // Reschedule itself to run again if not closed
            if (closed == false) {
                threadPool.schedule(interval, ThreadPool.Names.SAME, this);
            }
        }
    }
}

