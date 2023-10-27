/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.blob.v2;

import static io.crate.testing.Asserts.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.IOUtils;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BlobIndicesServiceTest extends ESTestCase {

    private BlobIndicesService blobIndicesService;
    private ClusterService clusterService;
    private TestThreadPool threadPool;

    @Before
    public void init() throws Exception {
        threadPool = new TestThreadPool("dummy");
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
        blobIndicesService = new BlobIndicesService(Settings.EMPTY, clusterService);
    }

    @After
    public void stop() throws Exception {
        IOUtils.closeWhileHandlingException(clusterService);
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }

    @Test
    public void testBlobComponentsAreNotCreatedForNonBlobIndex() throws Exception {
        IndexService indexService = mock(IndexService.class);
        Index index = new Index("dummy", UUIDs.randomBase64UUID());
        when(indexService.index()).thenReturn(index);
        blobIndicesService.afterIndexCreated(indexService);

        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.shardId()).thenReturn(new ShardId(index, 0));
        blobIndicesService.afterIndexShardCreated(indexShard);

        assertThat(blobIndicesService.indices).isEmpty();
    }
}
