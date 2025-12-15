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

package io.crate.execution.dml.upsert;


import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.UUID;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RelationMetadata;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.junit.Before;
import org.junit.Test;

import io.crate.common.unit.TimeValue;
import io.crate.execution.dml.upsert.ShardUpsertRequest.DuplicateKeyAction;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ScopedRef;
import io.crate.metadata.SearchPath;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.settings.SessionSettings;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class ShardUpsertRequestTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;
    private ScopedRef idRef;
    private ScopedRef nameRef;
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
    public void test_streaming_without_returnvalues() throws Exception {
        String[] assignmentColumns = new String[]{"id", "name"};
        UUID jobId = UUID.randomUUID();
        ScopedRef[] missingAssignmentColumns = new ScopedRef[]{idRef, nameRef};
        ShardUpsertRequest request = new ShardUpsertRequest.Builder(
            new SessionSettings("dummyUser", SearchPath.createSearchPathFrom("dummySchema")),
            TimeValue.timeValueSeconds(30),
            DuplicateKeyAction.UPDATE_OR_FAIL,
            false,
            assignmentColumns,
            missingAssignmentColumns,
            null,
            jobId).newRequest(shardId);

        request.add(123, ShardUpsertRequest.Item.forInsert(
            "99",
            List.of(),
            Translog.UNSET_AUTO_GENERATED_TIMESTAMP,
            missingAssignmentColumns,
            new Object[]{99, "Marvin"},
            null,
            0
        ));
        request.add(42, ShardUpsertRequest.Item.forInsert(
            "99",
            List.of(),
            Translog.UNSET_AUTO_GENERATED_TIMESTAMP,
            missingAssignmentColumns,
            new Object[]{99, "Marvin"},
            new Symbol[0],
            0
        ));
        request.add(5, new ShardUpsertRequest.Item(
            "42",
            new Symbol[]{Literal.of(42), Literal.of("Deep Thought")},
            null,
            2L,
            1L,
            5L,
            List.of(),
            Translog.UNSET_AUTO_GENERATED_TIMESTAMP,
            0
        ));

        for (var version : List.of(Version.V_6_1_1, Version.CURRENT)) {
            BytesStreamOutput out = new BytesStreamOutput();
            out.setVersion(version);
            request.writeTo(out);

            StreamInput in = out.bytes().streamInput();
            in.setVersion(version);
            ShardUpsertRequest request2 = new ShardUpsertRequest(e.schemas(), in);

            assertThat(request).isEqualTo(request2);
        }
    }

    @Test
    public void test_streaming_with_returnvalues() throws Exception {
        String[] assignmentColumns = new String[]{"id", "name"};
        UUID jobId = UUID.randomUUID();
        ScopedRef[] missingAssignmentColumns = new ScopedRef[]{idRef, nameRef};
        ShardUpsertRequest request = new ShardUpsertRequest.Builder(
            new SessionSettings("dummyUser", SearchPath.createSearchPathFrom("dummySchema")),
            TimeValue.timeValueSeconds(30),
            DuplicateKeyAction.UPDATE_OR_FAIL,
            false,
            assignmentColumns,
            missingAssignmentColumns,
            null,
            jobId
        ).newRequest(shardId);

        request.add(123, ShardUpsertRequest.Item.forInsert(
            "99",
            List.of(),
            Translog.UNSET_AUTO_GENERATED_TIMESTAMP,
            missingAssignmentColumns,
            new Object[]{99, "Marvin"},
            null,
            0
        ));
        request.add(42, ShardUpsertRequest.Item.forInsert(
            "99",
            List.of(),
            Translog.UNSET_AUTO_GENERATED_TIMESTAMP,
            missingAssignmentColumns,
            new Object[]{99, "Marvin"},
            new Symbol[0],
            0
        ));
        request.add(5, new ShardUpsertRequest.Item(
            "42",
            new Symbol[]{Literal.of(42), Literal.of("Deep Thought")},
            null,
            2L,
            1L,
            5L,
            List.of(),
            Translog.UNSET_AUTO_GENERATED_TIMESTAMP,
            0
        ));

        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        ShardUpsertRequest request2 = new ShardUpsertRequest(e.schemas(), in);

        assertThat(request).isEqualTo(request2);
    }

    @Test
    public void test_streaming_item_without_source_from_older_node() throws Exception {
        String[] assignmentColumns = new String[]{"id", "name"};
        UUID jobId = UUID.randomUUID();
        ScopedRef[] missingAssignmentColumns = new ScopedRef[]{idRef, nameRef};
        ShardUpsertRequest request = new ShardUpsertRequest.Builder(
            new SessionSettings("dummyUser", SearchPath.createSearchPathFrom("dummySchema")),
            TimeValue.timeValueSeconds(30),
            DuplicateKeyAction.UPDATE_OR_FAIL,
            false,
            assignmentColumns,
            missingAssignmentColumns,
            null,
            jobId
        ).newRequest(shardId);

        request.add(42, ShardUpsertRequest.Item.forInsert(
            "42",
            List.of(),
            Translog.UNSET_AUTO_GENERATED_TIMESTAMP,
            missingAssignmentColumns,
            new Object[]{42, "Marvin"},
            new Symbol[0],
            0
        ));

        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.V_5_2_0);
        request.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        in.setVersion(Version.V_5_2_0);
        ShardUpsertRequest request2 = new ShardUpsertRequest(e.schemas(), in);
        assertThat(request2.items().get(0).seqNo()).isEqualTo(SequenceNumbers.SKIP_ON_REPLICA);
    }

}
