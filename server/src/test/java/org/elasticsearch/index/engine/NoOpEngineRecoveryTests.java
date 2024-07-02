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
package org.elasticsearch.index.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.elasticsearch.cluster.routing.ShardRoutingHelper.initWithSameId;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import org.elasticsearch.cluster.routing.RecoverySource.ExistingStoreRecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.junit.Test;

public class NoOpEngineRecoveryTests extends IndexShardTestCase {

    @Test
    public void testRecoverFromNoOp() throws IOException {
        final int nbDocs = scaledRandomIntBetween(1, 100);

        final IndexShard indexShard = newStartedShard(true);
        for (int i = 0; i < nbDocs; i++) {
            indexDoc(indexShard, String.valueOf(i));
        }
        indexShard.close("test", true);

        final ShardRouting shardRouting = indexShard.routingEntry();
        IndexShard primary = reinitShard(indexShard, initWithSameId(shardRouting, ExistingStoreRecoverySource.INSTANCE),
            indexShard.indexSettings().getIndexMetadata(), List.of(idxSettings -> Optional.of(NoOpEngine::new)));
        recoverShardFromStore(primary);
        assertThat(primary.getMaxSeqNoOfUpdatesOrDeletes()).isEqualTo(primary.seqNoStats().getMaxSeqNo());
        assertThat(primary.docStats().getCount()).isEqualTo(nbDocs);

        IndexShard replica = newShard(false, Settings.EMPTY, List.of(idxSettings -> Optional.of(NoOpEngine::new)));
        recoverReplica(replica, primary, true);
        assertThat(replica.getMaxSeqNoOfUpdatesOrDeletes()).isEqualTo(replica.seqNoStats().getMaxSeqNo());
        assertThat(replica.docStats().getCount()).isEqualTo(nbDocs);
        closeShards(primary, replica);
    }
}
