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

package io.crate.execution.dml.upsert;

import io.crate.execution.dml.upsert.ShardUpsertRequest.DuplicateKeyAction;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Schemas;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.shard.ShardId;
import org.junit.Test;

import java.util.UUID;

import static org.hamcrest.Matchers.equalTo;

public class ShardUpsertRequestTest extends CrateUnitTest {

    private static final RelationName CHARACTERS_IDENTS = new RelationName(Schemas.DOC_SCHEMA_NAME, "characters");

    private static final Reference ID_REF = new Reference(
        new ReferenceIdent(CHARACTERS_IDENTS, "id"), RowGranularity.DOC, DataTypes.INTEGER);
    private static final Reference NAME_REF = new Reference(
        new ReferenceIdent(CHARACTERS_IDENTS, "name"), RowGranularity.DOC, DataTypes.STRING);

    @Test
    public void testStreaming() throws Exception {
        ShardId shardId = new ShardId("test", UUIDs.randomBase64UUID(), 1);
        String[] assignmentColumns = new String[]{"id", "name"};
        UUID jobId = UUID.randomUUID();
        Reference[] missingAssignmentColumns = new Reference[]{ID_REF, NAME_REF};
        ShardUpsertRequest request = new ShardUpsertRequest.Builder(
            DuplicateKeyAction.UPDATE_OR_FAIL,
            false,
            assignmentColumns,
            missingAssignmentColumns,
            jobId,
            false
        ).newRequest(shardId);
        request.validateConstraints(false);

        request.add(123, new ShardUpsertRequest.Item(
            "99",
            null,
            new Object[]{99, new BytesRef("Marvin")},
            null));
        request.add(42, new ShardUpsertRequest.Item(
            "99",
            new Symbol[0],
            new Object[]{99, new BytesRef("Marvin")},
            null));
        request.add(5, new ShardUpsertRequest.Item(
            "42",
            new Symbol[]{Literal.of(42), Literal.of("Deep Thought")},
            null,
            2L));

        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        ShardUpsertRequest request2 = new ShardUpsertRequest();
        request2.readFrom(in);

        assertThat(request, equalTo(request2));
    }

}
