/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.executor.transport;

import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TableIdent;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.shard.ShardId;
import org.junit.Test;

import java.util.UUID;

import static org.hamcrest.Matchers.equalTo;

public class ShardUpsertRequestTest extends CrateUnitTest {

    TableIdent charactersIdent = new TableIdent(null, "characters");

    Reference idRef = new Reference(
            new ReferenceIdent(charactersIdent, "id"), RowGranularity.DOC, DataTypes.INTEGER);
    Reference nameRef = new Reference(
            new ReferenceIdent(charactersIdent, "name"), RowGranularity.DOC, DataTypes.STRING);

    @Test
    public void testStreaming() throws Exception {
        ShardId shardId = new ShardId("test", 1);
        String[] assignmentColumns = new String[]{"id", "name"};
        UUID jobId = UUID.randomUUID();
        Reference[] missingAssignmentColumns = new Reference[]{idRef, nameRef};
        ShardUpsertRequest request = new ShardUpsertRequest(
                shardId,
                assignmentColumns,
                missingAssignmentColumns,
                "42",
                jobId);
        request.validateConstraints(false);

        request.add(123, new ShardUpsertRequest.Item(
                "99",
                null,
                new Object[]{99, new BytesRef("Marvin")},
                null));
        request.add(5, new ShardUpsertRequest.Item(
                "42",
                new Symbol[]{Literal.newLiteral(42), Literal.newLiteral("Deep Thought") },
                null,
                2L));

        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);

        StreamInput in = StreamInput.wrap(out.bytes());
        ShardUpsertRequest request2 = new ShardUpsertRequest();
        request2.readFrom(in);

        assertThat(request, equalTo(request2));
    }

}
