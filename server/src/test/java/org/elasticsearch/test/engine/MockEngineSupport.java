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
package org.elasticsearch.test.engine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.AssertingDirectoryReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.AssertingIndexSearcher;
import org.apache.lucene.search.QueryCache;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.IdentityHashMap;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Support class to build MockEngines like {@link MockInternalEngine}
 * since they need to subclass the actual engine
 */
public final class MockEngineSupport {

    /**
     * Allows tests to wrap an index reader randomly with a given ratio. This
     * is disabled by default ie. {@code 0.0d} since reader wrapping is insanely
     * slow if {@link AssertingDirectoryReader} is used.
     */
    public static final Setting<Double> WRAP_READER_RATIO =
        Setting.doubleSetting("index.engine.mock.random.wrap_reader_ratio", 0.0d, 0.0d, Property.IndexScope);
    /**
     * Allows tests to prevent an engine from being flushed on close ie. to test translog recovery...
     */
    public static final Setting<Boolean> DISABLE_FLUSH_ON_CLOSE =
        Setting.boolSetting("index.mock.disable_flush_on_close", false, Property.IndexScope);


    private final AtomicBoolean closing = new AtomicBoolean(false);
    private final Logger logger = LogManager.getLogger(Engine.class);
    private final ShardId shardId;
    private final QueryCache filterCache;
    private final QueryCachingPolicy filterCachingPolicy;
    private final InFlightSearchers inFlightSearchers;
    private final MockContext mockContext;
    private final boolean disableFlushOnClose;

    public boolean isFlushOnCloseDisabled() {
        return disableFlushOnClose;
    }


    public static class MockContext {
        private final Random random;
        private final boolean wrapReader;
        private final Class<? extends FilterDirectoryReader> wrapper;
        private final Settings indexSettings;

        public MockContext(Random random, boolean wrapReader, Class<? extends FilterDirectoryReader> wrapper, Settings indexSettings) {
            this.random = random;
            this.wrapReader = wrapReader;
            this.wrapper = wrapper;
            this.indexSettings = indexSettings;
        }
    }

    public MockEngineSupport(EngineConfig config, Class<? extends FilterDirectoryReader> wrapper) {
        Settings settings = config.getIndexSettings().getSettings();
        shardId = config.getShardId();
        filterCache = config.getQueryCache();
        filterCachingPolicy = config.getQueryCachingPolicy();
        final long seed =  config.getIndexSettings().getValue(ESIntegTestCase.INDEX_TEST_SEED_SETTING);
        Random random = new Random(seed);
        final double ratio = WRAP_READER_RATIO.get(settings);
        boolean wrapReader = random.nextDouble() < ratio;
        if (logger.isTraceEnabled()) {
            logger.trace("Using [{}] for shard [{}] seed: [{}] wrapReader: [{}]", this.getClass().getName(), shardId, seed, wrapReader);
        }
        mockContext = new MockContext(random, wrapReader, wrapper, settings);
        this.inFlightSearchers = new InFlightSearchers();
        LuceneTestCase.closeAfterSuite(inFlightSearchers); // only one suite closeable per Engine
        this.disableFlushOnClose = DISABLE_FLUSH_ON_CLOSE.get(settings);
    }

    enum CloseAction {
        FLUSH_AND_CLOSE,
        CLOSE;
    }


    /**
     * Returns the CloseAction to execute on the actual engine. Note this method changes the state on
     * the first call and treats subsequent calls as if the engine passed is already closed.
     */
    public CloseAction flushOrClose(CloseAction originalAction) throws IOException {
        /*
         * only do the random thing if we are the first call to this since
         * super.flushOnClose() calls #close() again and then we might end
         * up with a stackoverflow.
         */
        if (closing.compareAndSet(false, true)) {
            if (mockContext.random.nextBoolean()) {
                return CloseAction.FLUSH_AND_CLOSE;
            } else {
                return CloseAction.CLOSE;
            }
        } else {
            return originalAction;
        }
    }

    public AssertingIndexSearcher newSearcher(Engine.Searcher searcher) throws EngineException {
        IndexReader reader = searcher.reader();
        IndexReader wrappedReader = reader;
        assert reader != null;
        if (reader instanceof DirectoryReader && mockContext.wrapReader) {
            wrappedReader = wrapReader((DirectoryReader) reader);
        }
        // this executes basic query checks and asserts that weights are normalized only once etc.
        final AssertingIndexSearcher assertingIndexSearcher = new AssertingIndexSearcher(mockContext.random, wrappedReader);
        assertingIndexSearcher.setSimilarity(searcher.searcher().getSimilarity());
        assertingIndexSearcher.setQueryCache(filterCache);
        assertingIndexSearcher.setQueryCachingPolicy(filterCachingPolicy);
        return assertingIndexSearcher;
    }

    private DirectoryReader wrapReader(DirectoryReader reader) {
        try {
            Constructor<?>[] constructors = mockContext.wrapper.getConstructors();
            Constructor<?> nonRandom = null;
            for (Constructor<?> constructor : constructors) {
                Class<?>[] parameterTypes = constructor.getParameterTypes();
                if (parameterTypes.length > 0 && parameterTypes[0] == DirectoryReader.class) {
                    if (parameterTypes.length == 1) {
                        nonRandom = constructor;
                    } else if (parameterTypes.length == 2 && parameterTypes[1] == Settings.class) {

                        return (DirectoryReader) constructor.newInstance(reader, mockContext.indexSettings);
                    }
                }
            }
            if (nonRandom != null) {
                return (DirectoryReader) nonRandom.newInstance(reader);
            }
        } catch (Exception e) {
            throw new ElasticsearchException("Can not wrap reader", e);
        }
        return reader;
    }

    public abstract static class DirectoryReaderWrapper extends FilterDirectoryReader {
        protected final SubReaderWrapper subReaderWrapper;

        public DirectoryReaderWrapper(DirectoryReader in, SubReaderWrapper subReaderWrapper) throws IOException {
            super(in, subReaderWrapper);
            this.subReaderWrapper = subReaderWrapper;
        }

    }

    public Engine.Searcher wrapSearcher(Engine.Searcher engineSearcher) {
        final AssertingIndexSearcher assertingIndexSearcher = newSearcher(engineSearcher);
        assertingIndexSearcher.setSimilarity(engineSearcher.searcher().getSimilarity());
        /*
         * pass the original searcher to the super.newSearcher() method to
         * make sure this is the searcher that will be released later on.
         * If we wrap an index reader here must not pass the wrapped version
         * to the manager on release otherwise the reader will be closed too
         * early. - good news, stuff will fail all over the place if we don't
         * get this right here
         */
        SearcherCloseable closeable = new SearcherCloseable(engineSearcher, logger, inFlightSearchers);
        return new Engine.Searcher(engineSearcher.source(), assertingIndexSearcher, closeable);
    }

    private static final class InFlightSearchers implements Closeable {

        private final IdentityHashMap<Object, RuntimeException> openSearchers = new IdentityHashMap<>();

        @Override
        public synchronized void close() {
            if (openSearchers.isEmpty() == false) {
                AssertionError error = new AssertionError("Unreleased searchers found");
                for (RuntimeException ex : openSearchers.values()) {
                    error.addSuppressed(ex);
                }
                throw error;
            }
        }

        void add(Object key, String source) {
            final RuntimeException ex = new RuntimeException("Unreleased Searcher, source [" + source+ "]");
            synchronized (this) {
                openSearchers.put(key, ex);
            }
        }

        synchronized void remove(Object key) {
            openSearchers.remove(key);
        }
    }

    private static final class SearcherCloseable implements Closeable {
        private final Engine.Searcher wrappedSearcher;
        private final InFlightSearchers inFlightSearchers;
        private RuntimeException firstReleaseStack;
        private final Object lock = new Object();
        private final int initialRefCount;
        private final Logger logger;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        SearcherCloseable(final Engine.Searcher wrappedSearcher, Logger logger, InFlightSearchers inFlightSearchers) {
            // we only use the given index searcher here instead of the IS of the wrapped searcher. the IS might be a wrapped searcher
            // with a wrapped reader.
            this.wrappedSearcher = wrappedSearcher;
            this.logger = logger;
            initialRefCount = wrappedSearcher.reader().getRefCount();
            this.inFlightSearchers = inFlightSearchers;
            assert initialRefCount > 0 :
                "IndexReader#getRefCount() was [" + initialRefCount + "] expected a value > [0] - reader is already closed";
            inFlightSearchers.add(this, wrappedSearcher.source());
        }

        @Override
        public void close() {
            synchronized (lock) {
                if (closed.compareAndSet(false, true)) {
                    inFlightSearchers.remove(this);
                    firstReleaseStack = new RuntimeException();
                    final int refCount = wrappedSearcher.reader().getRefCount();
                    /*
                     * this assert seems to be paranoid but given LUCENE-5362 we
                     * better add some assertions here to make sure we catch any
                     * potential problems.
                     */
                    assert refCount > 0 : "IndexReader#getRefCount() was [" + refCount + "] expected a value > [0] - reader is already "
                        + " closed. Initial refCount was: [" + initialRefCount + "]";
                    try {
                        wrappedSearcher.close();
                    } catch (RuntimeException ex) {
                        logger.debug("Failed to release searcher", ex);
                        throw ex;
                    }
                } else {
                    AssertionError error = new AssertionError("Released Searcher more than once, source [" + wrappedSearcher.source()
                        + "]");
                    error.initCause(firstReleaseStack);
                    throw error;
                }
            }
        }
    }
}
