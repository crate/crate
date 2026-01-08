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
import java.util.Map;
import java.util.UUID;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RelationMetadata;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import io.crate.execution.dml.upsert.ShardUpsertRequest;
import io.crate.execution.dml.upsert.ShardUpsertRequest.DuplicateKeyAction;
import io.crate.execution.dml.upsert.ShardUpsertRequest.Item;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.Schemas;
import io.crate.metadata.SearchPath;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.settings.SessionSettings;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;

public class UpsertReplicaRequestTest extends CrateDummyClusterServiceUnitTest {

    SessionSettings sessionSettings = new SessionSettings("dummyUser", SearchPath.createSearchPathFrom("dummySchema"));
    UUID jobId = UUID.randomUUID();

    private SQLExecutor e;
    private Reference idRef;
    private Reference nameRef;
    private ShardId shardId;

    @Before
    public void setupTable() throws Exception {
        e = SQLExecutor.of(clusterService)
            .addTable("create table doc.characters (id int, name text)");
        DocTableInfo tableInfo = e.resolveTableInfo("characters");
        idRef = tableInfo.getReference(ColumnIdent.of("id"));
        nameRef = tableInfo.getReference(ColumnIdent.of("name"));

        Metadata metadata = clusterService.state().metadata();
        RelationMetadata relation = metadata.getRelation(tableInfo.ident());
        assertThat(relation).isNotNull();
        String indexUUID = relation.indexUUIDs().get(0);
        IndexMetadata index = metadata.index(indexUUID);
        assertThat(index).isNotNull();
        shardId = new ShardId(index.getIndex().getName(), index.getIndexUUID(), 1);
    }

    @Test
    public void test_can_deserialize_replica_request_sent_from_5_10_10() throws Exception {
        Reference[] insertColumns = new Reference[] { idRef, nameRef };
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
                var replicaRequest = UpsertReplicaRequest.readFrom(Mockito.mock(Schemas.class), in);
                assertThat(replicaRequest.columns()).containsExactly(
                    idRef,
                    nameRef
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
            List.of(idRef, nameRef),
            items
        );
        try (var out = new BytesStreamOutput()) {
            out.setVersion(Version.V_5_10_10);
            replicaRequest.writeTo(out);

            try (var in = out.bytes().streamInput()) {
                in.setVersion(Version.V_5_10_10);
                ShardUpsertRequest shardUpsertRequest = new ShardUpsertRequest(e.schemas(), in);
                assertThat(shardUpsertRequest.insertColumns()).containsExactly(
                    idRef,
                    nameRef
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

    @Test
    public void test_61_and_current_streaming() throws Exception {
        List<UpsertReplicaRequest.Item> items = List.of(
            new UpsertReplicaRequest.Item("1", new Object[] { 1, "Arthur" }, List.of("1"), 1, 2, 3),
            new UpsertReplicaRequest.Item("2", new Object[] { 2, "Trillian" }, List.of("2"), 4, 5, 6)
        );
        UpsertReplicaRequest replicaRequest = new UpsertReplicaRequest(
            shardId,
            jobId,
            sessionSettings,
            List.of(idRef, nameRef),
            items
        );
        for (var version : List.of(Version.V_6_1_0, Version.CURRENT)) {
            try (var out = new BytesStreamOutput()) {
                out.setVersion(version);
                replicaRequest.writeTo(out);
                try (var in = out.bytes().streamInput()) {
                    in.setVersion(version);
                    UpsertReplicaRequest request = UpsertReplicaRequest.readFrom(e.schemas(), in);
                    assertThat(request).isEqualTo(replicaRequest);
                    assertThat(request.columns()).isEqualTo(replicaRequest.columns());
                    assertThat(request.items()).isEqualTo(replicaRequest.items());
                }
            }
        }
    }

    /// Emulating an insert into a non-typed dynamic object column, ensuring that the data type from the stream is
    /// used instead of the (normally, dynamically) registered column reference type.
    @Test
    public void test_streaming_with_dynamic_object_members() throws Exception {
        // We define the inner type of the dynamic object directly, which would otherwise happen during mapping update
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table doc.t1 (id int, obj object as (a int))");
        DocTableInfo tableInfo = e.resolveTableInfo("t1");
        Metadata metadata = clusterService.state().metadata();
        RelationMetadata relation = metadata.getRelation(tableInfo.ident());
        assertThat(relation).isNotNull();
        String indexUUID = relation.indexUUIDs().get(0);
        IndexMetadata index = metadata.index(indexUUID);
        assertThat(index).isNotNull();
        ShardId shardId = new ShardId(index.getIndex().getName(), index.getIndexUUID(), 1);

        Reference idRef = tableInfo.getReference(ColumnIdent.of("id"));
        Reference objRefOriginal = tableInfo.getReference(ColumnIdent.of("obj"));
        // Override the inner type of 'a' to UNDEFINED to simulate a dynamic column
        Reference objRefWithUndefinedChild = objRefOriginal.withValueType(ObjectType.of(ColumnPolicy.DYNAMIC).setInnerType("a", DataTypes.UNDEFINED).build());

        List<UpsertReplicaRequest.Item> items = List.of(
            new UpsertReplicaRequest.Item("1", new Object[] { 1, Map.of("a", 1L) }, List.of("1"), 4, 5, 6)
        );
        UpsertReplicaRequest replicaRequest = new UpsertReplicaRequest(
            shardId,
            jobId,
            sessionSettings,
            List.of(idRef, objRefWithUndefinedChild),
            items
        );

        try (var out = new BytesStreamOutput()) {
            replicaRequest.writeTo(out);
            try (var in = out.bytes().streamInput()) {
                UpsertReplicaRequest request = UpsertReplicaRequest.readFrom(e.schemas(), in);
                assertThat(request).isEqualTo(replicaRequest);
                assertThat(request.columns()).isEqualTo(replicaRequest.columns());
                assertThat(request.items()).isEqualTo(replicaRequest.items());
            }
        }
    }
}

