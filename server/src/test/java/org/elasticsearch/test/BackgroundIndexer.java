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

package org.elasticsearch.test;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import io.crate.common.unit.TimeValue;
import io.crate.testing.SQLTransportExecutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.junit.Assert;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Locale;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;

public class BackgroundIndexer implements AutoCloseable {

    private final Logger logger = LogManager.getLogger(getClass());

    final Thread[] writers;
    final SQLTransportExecutor sqlExecutor;
    final CountDownLatch stopLatch;
    final Collection<Exception> failures = new ArrayList<>();
    final AtomicBoolean stop = new AtomicBoolean(false);
    final AtomicLong idGenerator = new AtomicLong();
    final CountDownLatch startLatch = new CountDownLatch(1);
    final AtomicBoolean hasBudget = new AtomicBoolean(false); // when set to true, writers will acquire writes from a semaphore
    final Semaphore availableBudget = new Semaphore(0);
    private final Set<String> ids = ConcurrentCollections.newConcurrentSet();
    private volatile Consumer<Exception> failureAssertion = null;
    final String table;

    volatile int minFieldSize = 10;
    volatile int maxFieldSize = 140;

    /**
     * Start indexing in the background using a random number of threads. Indexing will be paused after numOfDocs docs has
     * been indexed.
     *
     * @param table     table name to write data
     * @param column    column name to write data
     * @param client    client to use
     * @param numOfDocs number of document to index before pausing. Set to -1 to have no limit.
     */
    public BackgroundIndexer(String table, String column, SQLTransportExecutor client, int numOfDocs) {
        this(table, column, client, numOfDocs, RandomizedTest.scaledRandomIntBetween(2, 5));
    }

    /**
     * Start indexing in the background using a given number of threads. Indexing will be paused after numOfDocs docs has
     * been indexed.
     *
     * @param table       table name to write data
     * @param column      column name to write data
     * @param client      client to use
     * @param numOfDocs   number of document to index before pausing. Set to -1 to have no limit.
     * @param writerCount number of indexing threads to use
     */
    public BackgroundIndexer(String table, String column, SQLTransportExecutor client, int numOfDocs, final int writerCount) {
        this(table, column, client, numOfDocs, writerCount, true, null);
    }

    /**
     * Start indexing in the background using a given number of threads. Indexing will be paused after numOfDocs docs has
     * been indexed.
     *
     * @param table       table name to write data
     * @param column      column name to write data
     * @param numOfDocs   number of document to index before pausing. Set to -1 to have no limit.
     * @param writerCount number of indexing threads to use
     * @param autoStart   set to true to start indexing as soon as all threads have been created.
     * @param random      random instance to use
     */
    public BackgroundIndexer(final String table, final String column, final SQLTransportExecutor sqlExecutor, final int numOfDocs, final int writerCount,
                             boolean autoStart, Random random) {
        if (random == null) {
            random = RandomizedTest.getRandom();
        }
        this.table = table;
        this.sqlExecutor = sqlExecutor;
        writers = new Thread[writerCount];
        stopLatch = new CountDownLatch(writers.length);
        logger.info("--> creating {} indexing threads (auto start: [{}], numOfDocs: [{}])", writerCount, autoStart, numOfDocs);
        for (int i = 0; i < writers.length; i++) {
            final int indexerId = i;
            final boolean batch = random.nextBoolean();
            final Random threadRandom = new Random(random.nextLong());
            writers[i] = new Thread() {
                @Override
                public void run() {
                    long id = -1;
                    try {
                        startLatch.await();
                        logger.info("**** starting indexing thread {}", indexerId);
                        while (!stop.get()) {
                            if (batch) {
                                int batchSize = threadRandom.nextInt(20) + 1;
                                if (hasBudget.get()) {
                                    // always try to get at least one
                                    batchSize = Math.max(Math.min(batchSize, availableBudget.availablePermits()), 1);
                                    if (!availableBudget.tryAcquire(batchSize, 250, TimeUnit.MILLISECONDS)) {
                                        // time out -> check if we have to stop.
                                        continue;
                                    }
                                }
                                var insertData = new Object[batchSize];
                                for (int i = 0; i < batchSize; i++) {
                                    insertData[i] = generateValues(idGenerator.incrementAndGet(), threadRandom);
                                }
                                try {
                                    var insert = String.format(Locale.ENGLISH, "insert into %s (id, %s) (select * from unnest(?)) returning _id", table, column);
                                    var response = sqlExecutor.exec(insert, insertData);
                                    for (var generatedId : response.rows()[0]) {
                                        ids.add((String) generatedId);
                                    }
                                } catch (Exception e) {
                                    if (ignoreIndexingFailures == false) {
                                        throw e;
                                    }
                                }
                            } else {
                                if (hasBudget.get() && !availableBudget.tryAcquire(250, TimeUnit.MILLISECONDS)) {
                                    // time out -> check if we have to stop.
                                    continue;
                                }
                                try {
                                    var insert = String.format(Locale.ENGLISH, "insert into %s (id, %s) values(?, ?) returning _id", table, column);
                                    var response = sqlExecutor.exec(insert, generateValues(idGenerator.incrementAndGet(), threadRandom));
                                    ids.add((String) response.rows()[0][0]);
                                } catch (Exception e) {
                                    if (ignoreIndexingFailures == false) {
                                        throw e;
                                    }
                                }
                            }
                        }
                        logger.info("**** done indexing thread {}  stop: {} numDocsIndexed: {}", indexerId, stop.get(), ids.size());
                    } catch (Exception e) {
                        trackFailure(e);
                        final long docId = id;
                        logger.warn(
                            (Supplier<?>)
                                () -> new ParameterizedMessage("**** failed indexing thread {} on doc id {}", indexerId, docId), e);
                    } finally {
                        stopLatch.countDown();
                    }
                }
            };
            writers[i].start();
        }

        if (autoStart) {
            start(numOfDocs);
        }
    }

    private void trackFailure(Exception e) {
        synchronized (failures) {
            if (failureAssertion != null) {
                failureAssertion.accept(e);
            } else {
                failures.add(e);
            }
        }
    }

    private Object[] generateValues(@Nullable Long id, Random random) throws IOException {
        int contentLength = RandomNumbers.randomIntBetween(random, minFieldSize, maxFieldSize);
        StringBuilder text = new StringBuilder(contentLength);
        while (text.length() < contentLength) {
            int tokenLength = RandomNumbers.randomIntBetween(random, 1, Math.min(contentLength - text.length(), 10));
            text.append(" ").append(RandomStrings.randomRealisticUnicodeOfCodepointLength(random, tokenLength));
        }
        return new Object[]{id, text.toString()};
    }

    private volatile TimeValue timeout = new TimeValue(1, TimeUnit.MINUTES);

    public void setRequestTimeout(TimeValue timeout) {
        this.timeout = timeout;
    }

    private volatile boolean ignoreIndexingFailures;

    public void setIgnoreIndexingFailures(boolean ignoreIndexingFailures) {
        this.ignoreIndexingFailures = ignoreIndexingFailures;
    }

    private void setBudget(int numOfDocs) {
        logger.debug("updating budget to [{}]", numOfDocs);
        if (numOfDocs >= 0) {
            hasBudget.set(true);
            availableBudget.release(numOfDocs);
        } else {
            hasBudget.set(false);
        }

    }

    /**
     * Start indexing
     *
     * @param numOfDocs number of document to index before pausing. Set to -1 to have no limit.
     */
    public void start(int numOfDocs) {
        assert !stop.get() : "background indexer can not be started after it has stopped";
        setBudget(numOfDocs);
        startLatch.countDown();
    }

    /** Pausing indexing by setting current document limit to 0 */
    public void pauseIndexing() {
        availableBudget.drainPermits();
        setBudget(0);
    }

    /**
     * Continue indexing after it has paused.
     *
     * @param numOfDocs number of document to index before pausing. Set to -1 to have no limit.
     */
    public void continueIndexing(int numOfDocs) {
        setBudget(numOfDocs);
    }

    /** Stop all background threads but don't wait for ongoing indexing operations to finish * */
    public void stop() {
        stop.set(true);
    }

    public void awaitStopped() throws InterruptedException {
        assert stop.get();
        Assert.assertThat("timeout while waiting for indexing threads to stop", stopLatch.await(6, TimeUnit.MINUTES), equalTo(true));
        if (failureAssertion == null) {
            assertNoFailures();
        }
    }

    /** Stop all background threads and wait for ongoing indexing operations to finish * */
    public void stopAndAwaitStopped() throws InterruptedException {
        stop();
        awaitStopped();
    }

    public long totalIndexedDocs() {
        return ids.size();
    }

    public void assertNoFailures() {
        synchronized (failures) {
            Assert.assertThat(failures, emptyIterable());
        }
    }

    /**
     * Set a consumer that can be used to run assertions on failures during indexing. If such a consumer is set then it disables adding
     * failures to {@link #failures}. Should be used if the number of expected failures during indexing could become very large.
     */
    public void setFailureAssertion(Consumer<Exception> failureAssertion) {
        synchronized (failures) {
            this.failureAssertion = failureAssertion;
            boolean success = false;
            try {
                for (Exception failure : failures) {
                    failureAssertion.accept(failure);
                }
                failures.clear();
                success = true;
            } finally {
                if (success == false) {
                    stop();
                }
            }
        }
    }

    @Override
    public void close() throws Exception {
        stop();
    }

    /**
     * Returns the ID set of all documents indexed by this indexer run
     */
    public Set<String> getIds() {
        return this.ids;
    }
}
