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
package org.elasticsearch.test.hamcrest;

import static org.assertj.core.api.Assertions.assertThat;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;

public class ElasticsearchAssertions {

    public static void assertNoTimeout(ClusterHealthResponse response) {
        assertThat(response.isTimedOut())
          .as("ClusterHealthResponse has timed out - returned: [" + response + "]")
          .isFalse();
    }

    public static void assertAcked(AcknowledgedResponse response) {
        assertThat(response.isAcknowledged()).as(response.getClass().getSimpleName() + " failed - not acked").isTrue();
    }

    /**
     * Assert that an index creation was fully acknowledged, meaning that both the index creation cluster
     * state update was successful and that the requisite number of shard copies were started before returning.
     */
    public static void assertAcked(CreateIndexResponse response) {
        assertThat(response.isAcknowledged()).as(response.getClass().getSimpleName() + " failed - not acked").isTrue();
        assertThat(response.isShardsAcknowledged()).as(response.getClass().getSimpleName() + " failed - index creation acked but not all shards were started").isTrue();
    }
}
