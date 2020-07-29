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
package org.elasticsearch.index.shard;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import java.io.IOException;

import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.index.engine.InternalEngine;
import org.junit.Test;

/**
 * Simple unit-test IndexShard related operations.
 */
public class IndexShardTests extends IndexShardTestCase {

    @Test
    public void testRecordsForceMerges() throws IOException {
        IndexShard shard = newStartedShard(true);
        final String initialForceMergeUUID = ((InternalEngine) shard.getEngine()).getForceMergeUUID();
        assertThat(initialForceMergeUUID, nullValue());
        final ForceMergeRequest firstForceMergeRequest = new ForceMergeRequest().maxNumSegments(1);
        shard.forceMerge(firstForceMergeRequest);
        final String secondForceMergeUUID = ((InternalEngine) shard.getEngine()).getForceMergeUUID();
        assertThat(secondForceMergeUUID, notNullValue());
        assertThat(secondForceMergeUUID, equalTo(firstForceMergeRequest.forceMergeUUID()));
        final ForceMergeRequest secondForceMergeRequest = new ForceMergeRequest().maxNumSegments(1);
        shard.forceMerge(secondForceMergeRequest);
        final String thirdForceMergeUUID = ((InternalEngine) shard.getEngine()).getForceMergeUUID();
        assertThat(thirdForceMergeUUID, notNullValue());
        assertThat(thirdForceMergeUUID, not(equalTo(secondForceMergeUUID)));
        assertThat(thirdForceMergeUUID, equalTo(secondForceMergeRequest.forceMergeUUID()));
        closeShards(shard);
    }
}
