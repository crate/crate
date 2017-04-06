/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.blob.v2;

import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesLifecycle;
import org.elasticsearch.test.cluster.NoopClusterService;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BlobIndicesServiceTest extends CrateUnitTest {

    @Test
    public void testBlobComponentsAreNotCreatedForNonBlobIndex() throws Exception {
        CompletableFuture<IndicesLifecycle.Listener> listenerFuture = new CompletableFuture<>();
        ThreadPool testingPool = new ThreadPool("testingPool");
        try {
            BlobIndicesService blobIndicesService = new BlobIndicesService(
                Settings.EMPTY,
                new NoopClusterService(),
                new IndicesLifecycle() {
                    @Override
                    public void addListener(Listener listener) {
                        listenerFuture.complete(listener);
                    }

                    @Override
                    public void removeListener(Listener listener) {

                    }
                },
                testingPool
            );
            IndicesLifecycle.Listener listener = listenerFuture.get(30, TimeUnit.SECONDS);

            IndexService indexService = mock(IndexService.class);
            when(indexService.index()).thenReturn(new Index("dummy"));
            listener.afterIndexCreated(indexService);

            IndexShard indexShard = mock(IndexShard.class);
            when(indexShard.indexService()).thenReturn(indexService);
            listener.afterIndexShardCreated(indexShard);

            assertThat(blobIndicesService.indices.keySet(), Matchers.empty());
        } finally {
            testingPool.shutdown();
            testingPool.awaitTermination(10, TimeUnit.SECONDS);
        }
    }
}
