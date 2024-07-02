/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.engine;

import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicInteger;

import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.IntegTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MaxDocsLimitIT extends IntegTestCase {

    private static final AtomicInteger maxDocs = new AtomicInteger();

    public static class TestEnginePlugin extends Plugin implements EnginePlugin {
        @Override
        public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
            return Optional.of(config -> {
                assert maxDocs.get() > 0 : "maxDocs is unset";
                return EngineTestCase.createEngine(config, maxDocs.get());
            });
        }
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(TestEnginePlugin.class);
        return plugins;
    }

    @Before
    public void setMaxDocs() {
        maxDocs.set(randomIntBetween(10, 100)); // Do not set this too low as we can fail to write the cluster state
        setIndexWriterMaxDocs(maxDocs.get());
    }

    @After
    public void restoreMaxDocs() {
        restoreIndexWriterMaxDocs();
    }

    @Test
    public void testMaxDocsLimit() throws Exception {
        cluster().ensureAtLeastNumDataNodes(1);
        execute("create table tbl (x int) clustered into 1 shards");
        Object[][] rows = new Object[maxDocs.get()][];
        for (int i = 0; i < rows.length; i++) {
            rows[i] = new Object[] { randomInt() };
        }
        long[] rowCounts = execute("insert into tbl (x) values (?)", rows);
        assertThat(rowCounts).hasSize(maxDocs.get());
        for (long rowCount : rowCounts) {
            assertThat(rowCount).isEqualTo(1L);
        }
        int rejectedDocs = between(2, 10);
        rows = new Object[rejectedDocs][];
        for (int i = 0; i < rows.length; i++) {
            rows[i] = new Object[] { randomInt() };
        }
        rowCounts = execute("insert into tbl (x) values (?)", rows);
        assertThat(rowCounts).hasSize(rejectedDocs);
        for (long rowCount : rowCounts) {
            assertThat(rowCount).isEqualTo(-2L);
        }

        execute("refresh table tbl");
        String id = (String) execute("select _id from tbl limit 1").rows()[0][0];
        assertThatThrownBy(() -> execute("delete from tbl where _id = ?", new Object[] { id }))
            .hasMessageContaining("Number of documents in the index can't exceed [" + maxDocs.get() + "]");
    }

    @Test
    public void testMaxDocsLimitConcurrently() throws Exception {
        cluster().ensureAtLeastNumDataNodes(1);
        execute("create table tbl (x int) clustered into 1 shards");
        int numRequests = between(maxDocs.get() + 1, maxDocs.get() * 2);
        int numThreads = between(2, 8);

        AtomicInteger completedRequests = new AtomicInteger();
        AtomicInteger numSuccess = new AtomicInteger();
        AtomicInteger numFailure = new AtomicInteger();
        Thread[] indexers = new Thread[numThreads];
        Phaser phaser = new Phaser(indexers.length);
        for (int i = 0; i < numThreads; i++) {
            indexers[i] = new Thread(() -> {
                phaser.arriveAndAwaitAdvance();
                while (completedRequests.incrementAndGet() <= numRequests) {
                    try {
                        var resp = execute("insert into tbl (x) values (?)", new Object[] { randomInt() });
                        numSuccess.incrementAndGet();
                        assertThat(resp).hasRowCount(1);
                    } catch (Exception e) {
                        numFailure.incrementAndGet();
                        assertThat(e).hasMessageContaining("Number of documents in the index can't exceed [" + maxDocs.get() + "]");
                    }
                }
            });
            indexers[i].start();
        }
        for (var thread : indexers) {
            thread.join();
        }
        assertThat(numFailure.get()).isGreaterThan(0);
        assertThat(numSuccess.get()).isBetween(1, maxDocs.get());

        execute("refresh table tbl");
        assertThat(execute("select count(*) from tbl")).hasRows(
            new Object[] { (long) numSuccess.get() }
        );
    }
}

