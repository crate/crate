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

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Locale;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.jetbrains.annotations.Nullable;

import com.carrotsearch.randomizedtesting.RandomizedTest;

import io.crate.common.collections.Sets;
import io.crate.common.unit.TimeValue;
import io.crate.testing.DataTypeTesting;
import io.crate.types.DataTypes;

public class BackgroundIndexer implements AutoCloseable {

    private final Logger logger = LogManager.getLogger(getClass());

    final Thread[] writers;
    final CountDownLatch stopLatch;
    final Collection<Exception> failures = new ArrayList<>();
    final AtomicBoolean stop = new AtomicBoolean(false);
    final AtomicLong idGenerator = new AtomicLong();
    final CountDownLatch startLatch = new CountDownLatch(1);
    final AtomicBoolean hasBudget = new AtomicBoolean(false); // when set to true, writers will acquire writes from a semaphore
    final Semaphore availableBudget = new Semaphore(0);
    private final Set<String> ids = Sets.newConcurrentHashSet();
    private volatile Consumer<Exception> failureAssertion = null;
    final String table;

    volatile int minFieldSize = 10;
    volatile int maxFieldSize = 140;


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
    public BackgroundIndexer(String schema,
                             String table,
                             String column,
                             String pgUrl,
                             int numOfDocs,
                             int writerCount,
                             boolean autoStart,
                             @Nullable Random random) {
        if (random == null) {
            random = RandomizedTest.getRandom();
        }
        this.table = table;
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
                    var textGenerator = DataTypeTesting.getDataGenerator(DataTypes.STRING);
                    Properties properties = new Properties();
                    properties.setProperty("user", "crate");
                    try (Connection conn = DriverManager.getConnection(pgUrl, properties)) {
                        conn.setSchema(schema);
                        PreparedStatement insertValues = conn.prepareStatement(String.format(
                            Locale.ENGLISH,
                            "insert into %s (id, %s) values (?, ?) returning _id",
                            table,
                            column
                        ));
                        PreparedStatement insertUnnest = conn.prepareStatement(String.format(
                            Locale.ENGLISH,
                            "insert into %s (id, %s) (select * from unnest(?, ?)) returning _id",
                            table,
                            column
                        ));
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
                                Object[] insertIds = new Object[batchSize];
                                Object[] strings = new Object[batchSize];
                                for (int i = 0; i < batchSize; i++) {
                                    insertIds[i] = idGenerator.incrementAndGet();
                                    strings[i] = textGenerator.get();
                                }
                                try {
                                    insertUnnest.setObject(1, conn.createArrayOf("bigint", insertIds));
                                    insertUnnest.setObject(2, conn.createArrayOf("text", strings));
                                    var result = insertUnnest.executeQuery();
                                    while (result.next()) {
                                        ids.add(result.getString(1));
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
                                    insertValues.setLong(1, idGenerator.incrementAndGet());
                                    insertValues.setObject(2, textGenerator.get());
                                    var result = insertValues.executeQuery();
                                    while (result.next()) {
                                        ids.add(result.getString(1));
                                    }
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
        assertThat(stopLatch.await(6, TimeUnit.MINUTES))
            .as("timeout while waiting for indexing threads to stop")
            .isTrue();
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
            assertThat(failures).isEmpty();
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
