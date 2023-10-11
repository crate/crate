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
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.common.unit.TimeValue;
import io.crate.execution.dml.upsert.ShardUpsertRequest.DuplicateKeyAction;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Schemas;
import io.crate.metadata.SearchPath;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.settings.SessionSettings;
import io.crate.types.DataTypes;

public class ShardUpsertRequestTest extends ESTestCase {

    private static final RelationName CHARACTERS_IDENTS = new RelationName(Schemas.DOC_SCHEMA_NAME, "characters");

    private static final SimpleReference ID_REF = new SimpleReference(
        new ReferenceIdent(CHARACTERS_IDENTS, "id"),
        RowGranularity.DOC,
        DataTypes.INTEGER,
        1,
        null);
    private static final SimpleReference NAME_REF = new SimpleReference(
        new ReferenceIdent(CHARACTERS_IDENTS, "name"),
        RowGranularity.DOC,
        DataTypes.STRING,
        2,
        null);

    @Test
    public void test_streaming_without_returnvalues() throws Exception {
        ShardId shardId = new ShardId("test", UUIDs.randomBase64UUID(), 1);
        String[] assignmentColumns = new String[]{"id", "name"};
        UUID jobId = UUID.randomUUID();
        SimpleReference[] missingAssignmentColumns = new SimpleReference[]{ID_REF, NAME_REF};
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
            new Object[]{99, "Marvin"},
            null
        ));
        request.add(42, ShardUpsertRequest.Item.forInsert(
            "99",
            List.of(),
            Translog.UNSET_AUTO_GENERATED_TIMESTAMP,
            new Object[]{99, "Marvin"},
            new Symbol[0]
        ));
        request.add(5, new ShardUpsertRequest.Item(
            "42",
            new Symbol[]{Literal.of(42), Literal.of("Deep Thought")},
            null,
            2L,
            1L,
            5L,
            List.of(),
            Translog.UNSET_AUTO_GENERATED_TIMESTAMP
        ));

        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        ShardUpsertRequest request2 = new ShardUpsertRequest(in);

        assertThat(request).isEqualTo(request2);
    }

    @Test
    public void test_streaming_with_returnvalues() throws Exception {
        ShardId shardId = new ShardId("test", UUIDs.randomBase64UUID(), 1);
        String[] assignmentColumns = new String[]{"id", "name"};
        UUID jobId = UUID.randomUUID();
        SimpleReference[] missingAssignmentColumns = new SimpleReference[]{ID_REF, NAME_REF};
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
            new Object[]{99, "Marvin"},
            null
        ));
        request.add(42, ShardUpsertRequest.Item.forInsert(
            "99",
            List.of(),
            Translog.UNSET_AUTO_GENERATED_TIMESTAMP,
            new Object[]{99, "Marvin"},
            new Symbol[0]
        ));
        request.add(5, new ShardUpsertRequest.Item(
            "42",
            new Symbol[]{Literal.of(42), Literal.of("Deep Thought")},
            null,
            2L,
            1L,
            5L,
            List.of(),
            Translog.UNSET_AUTO_GENERATED_TIMESTAMP
        ));

        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        ShardUpsertRequest request2 = new ShardUpsertRequest(in);

        assertThat(request).isEqualTo(request2);
    }

    @Test
    public void test_streaming_item_without_source_from_older_node() throws Exception {
        ShardId shardId = new ShardId("test", UUIDs.randomBase64UUID(), 1);
        String[] assignmentColumns = new String[]{"id", "name"};
        UUID jobId = UUID.randomUUID();
        SimpleReference[] missingAssignmentColumns = new SimpleReference[]{ID_REF, NAME_REF};
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
                new Object[]{42, "Marvin"},
                new Symbol[0]
        ));

        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.V_5_2_0);
        request.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        in.setVersion(Version.V_5_2_0);
        ShardUpsertRequest request2 = new ShardUpsertRequest(in);
        assertThat(request2.items().get(0).seqNo()).isEqualTo(SequenceNumbers.SKIP_ON_REPLICA);
    }
}
