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
package org.elasticsearch.indices;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.IntegTestCase;


@IntegTestCase.ClusterScope(numDataNodes = 1)
public class IndexingMemoryControllerIT extends IntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
            // small indexing buffer so that we can trigger refresh after buffering 100 deletes
            .put("indices.memory.index_buffer_size", "1kb").build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var classes = new HashSet<>(super.nodePlugins());
        classes.add(TestEnginePlugin.class);
        return classes;
    }

    protected boolean addMockInternalEngine() {
        return false;
    }

    public static class TestEnginePlugin extends Plugin implements EnginePlugin {

        EngineConfig engineConfigWithLargerIndexingMemory(EngineConfig config) {
            // We need to set a larger buffer for the IndexWriter; otherwise, it will flush before the IndexingMemoryController.
            Settings settings = Settings.builder().put(config.getIndexSettings().getSettings())
                .put("indices.memory.index_buffer_size", "10mb").build();
            IndexSettings indexSettings = new IndexSettings(config.getIndexSettings().getIndexMetadata(), settings);
            return new EngineConfig(config.getShardId(), config.getThreadPool(),
                                    indexSettings, config.getStore(), config.getMergePolicy(), config.getAnalyzer(),
                                    new CodecService(),
                                    config.getEventListener(), config.getQueryCache(),
                                    config.getQueryCachingPolicy(), config.getTranslogConfig(), config.getFlushMergesAfter(),
                                    config.getExternalRefreshListener(), config.getInternalRefreshListener(),
                                    config.getCircuitBreakerService(), config.getGlobalCheckpointSupplier(), config.retentionLeasesSupplier(),
                                    config.getPrimaryTermSupplier(), config.getTombstoneDocSupplier());
        }

        @Override
        public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
            return Optional.of(config -> new InternalEngine(engineConfigWithLargerIndexingMemory(config)));
        }
    }

    public void testDeletesAloneCanTriggerRefresh() throws Exception {
        // Disable refresh interval to make sure it won't be executed while the test is running
        execute("create table doc.test(x int) clustered into 1 shards with(number_of_replicas=0, refresh_interval='-1')");

        for (int i = 0; i < 100; i++) {
            execute("insert into doc.test(x) values(?)", new Object[]{i});
        }
        // Force merge so we know all merges are done before we start deleting:
        execute("optimize table doc.test");

        for (int i = 0; i < 100; i++) {
            execute("delete from doc.test where x = ?", new Object[]{i});
        }

        // need to assert busily as IndexingMemoryController refreshes in background
        assertBusy(() -> {
            execute("select count(*) from doc.test");
            assertThat(response.rows()[0][0], is(0L));
        });
    }
}
