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

package io.crate.execution.dml;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.UUID;

import org.elasticsearch.Version;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.junit.Test;

import io.crate.execution.dml.upsert.ShardUpsertRequest;
import io.crate.execution.dml.upsert.ShardUpsertRequest.DuplicateKeyAction;
import io.crate.execution.dml.upsert.ShardUpsertRequest.Item;
import io.crate.execution.dml.upsert.ShardUpsertRequestTest;
import io.crate.metadata.Reference;
import io.crate.metadata.SearchPath;
import io.crate.metadata.settings.SessionSettings;

public class UpsertReplicaRequestTest {

    ShardId shardId = new ShardId("test", UUIDs.randomBase64UUID(), 1);
    SessionSettings sessionSettings = new SessionSettings("dummyUser", SearchPath.createSearchPathFrom("dummySchema"));
    UUID jobId = UUID.randomUUID();

    @Test
    public void test_can_deserialize_replica_request_sent_from_5_10_10() throws Exception {
        Reference[] insertColumns = new Reference[] { ShardUpsertRequestTest.ID_REF, ShardUpsertRequestTest.NAME_REF };
        ShardUpsertRequest request = new ShardUpsertRequest(
            shardId,
            jobId,
            true,
            DuplicateKeyAction.IGNORE,
            sessionSettings,
            null,
            insertColumns,
            null
        );
        request.add(
            0,
            Item.forInsert(
                "1",
                List.of("1"),
                Translog.UNSET_AUTO_GENERATED_TIMESTAMP,
                insertColumns,
                new Object[] { 1, "Arthur" },
                null,
                200
            ));
        request.add(
            0,
            Item.forInsert(
                "2",
                List.of("2"),
                Translog.UNSET_AUTO_GENERATED_TIMESTAMP,
                insertColumns,
                new Object[] { 2, "Trillian" },
                null,
                400
            ));
        try (var out = new BytesStreamOutput()) {
            out.setVersion(Version.V_5_10_10);
            request.writeTo(out);

            try (var in = out.bytes().streamInput()) {
                in.setVersion(Version.V_5_10_10);
                UpsertReplicaRequest replicaRequest = UpsertReplicaRequest.readFrom(in);
                assertThat(replicaRequest.columns()).containsExactly(
                    ShardUpsertRequestTest.ID_REF,
                    ShardUpsertRequestTest.NAME_REF
                );
                assertThat(replicaRequest.items()).hasSize(2);
            }
        }
    }

    @Test
    public void test_5_10_10_nodes_can_read_replica_request_as_old_primary_request() throws Exception {
        List<UpsertReplicaRequest.Item> items = List.of(
            new UpsertReplicaRequest.Item("1", new Object[] { 1, "Arthur" }, List.of("1"), 1, 2, 3),
            new UpsertReplicaRequest.Item("2", new Object[] { 2, "Trillian" }, List.of("2"), 4, 5, 6)
        );
        UpsertReplicaRequest replicaRequest = new UpsertReplicaRequest(
            shardId,
            jobId,
            sessionSettings,
            List.of(ShardUpsertRequestTest.ID_REF, ShardUpsertRequestTest.NAME_REF),
            items
        );
        try (var out = new BytesStreamOutput()) {
            out.setVersion(Version.V_5_10_10);
            replicaRequest.writeTo(out);

            try (var in = out.bytes().streamInput()) {
                in.setVersion(Version.V_5_10_10);
                ShardUpsertRequest shardUpsertRequest = new ShardUpsertRequest(in);
                assertThat(shardUpsertRequest.insertColumns()).containsExactly(
                    ShardUpsertRequestTest.ID_REF,
                    ShardUpsertRequestTest.NAME_REF
                );
                assertThat(shardUpsertRequest.items()).hasSize(2);
                for (int i = 0; i < items.size(); i++) {
                    Item item = shardUpsertRequest.items().get(i);
                    var replicaItem = items.get(i);
                    assertThat(item.seqNo()).isEqualTo(replicaItem.seqNo());
                    assertThat(item.primaryTerm()).isEqualTo(replicaItem.primaryTerm());
                    assertThat(item.version()).isEqualTo(replicaItem.version());
                }
            }
        }
    }
}

