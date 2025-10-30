/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.replication.logical.seqno;

import static io.crate.testing.Asserts.assertThat;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.index.shard.ShardId;
import org.junit.Test;

public class RetentionLeaseHelperTest {

    /**
     * Ensures the retention lease id format. This is important for logical-replication to survive upgrades/running
     * between mixed version clusters.
     * If it changes, existing logical replications between mixed versions won't work anymore as the id will not match
     * the id used on the cluster with a different version.
     */
    @Test
    public void test_retention_lease_id_for_shard() {
        ShardId shardId = new ShardId(
            "dummy",
            UUIDs.randomBase64UUID(),
            0
        );

        String leaseId = RetentionLeaseHelper.retentionLeaseIdForShard("cluster1", shardId);
        assertThat(leaseId).isEqualTo("logical_replication:cluster1:[dummy][0]");
    }
}
