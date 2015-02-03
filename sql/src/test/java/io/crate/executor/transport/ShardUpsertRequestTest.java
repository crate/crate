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

import io.crate.Constants;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.TableIdent;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.index.shard.ShardId;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

public class ShardUpsertRequestTest {

    TableIdent charactersIdent = new TableIdent(null, "characters");

    Reference idRef = new Reference(new ReferenceInfo(
            new ReferenceIdent(charactersIdent, "id"), RowGranularity.DOC, DataTypes.INTEGER));
    Reference nameRef = new Reference(new ReferenceInfo(
            new ReferenceIdent(charactersIdent, "name"), RowGranularity.DOC, DataTypes.STRING));

    @Test
    public void testStreaming() throws Exception {
        ShardId shardId = new ShardId("test", 1);
        String[] assignmentColumns = new String[]{"id", "name"};
        Reference[] missingAssignmentColumns = new Reference[]{idRef, nameRef};
        ShardUpsertRequest request = new ShardUpsertRequest(
                shardId,
                assignmentColumns,
                missingAssignmentColumns);

        request.add(123, "99",
                null,
                new Object[]{99, new BytesRef("Marvin")},
                null, null);
        request.add(5, "42",
                new Symbol[]{Literal.newLiteral(42), Literal.newLiteral("Deep Thought") },
                null,
                2L, "42");


        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);

        BytesStreamInput in = new BytesStreamInput(out.bytes());
        ShardUpsertRequest request2 = new ShardUpsertRequest();
        request2.readFrom(in);

        assertThat(request2.index(), is(shardId.getIndex()));
        assertThat(request2.shardId(), is(shardId.id()));
        assertThat(request2.updateColumns(), is(assignmentColumns));
        assertThat(request2.insertColumns(), is(missingAssignmentColumns));

        assertThat(request2.locations().size(), is(2));
        assertThat(request2.locations().get(0), is(123));
        assertThat(request2.locations().get(1), is(5));

        assertThat(request2.items().size(), is(2));

        ShardUpsertRequest.Item item1 = request2.items().get(0);
        assertThat(item1.id(), is("99"));
        assertNull(item1.updateAssignments());
        assertThat(item1.insertValues(), is(new Object[]{99, new BytesRef("Marvin")}));
        assertNull(item1.routing());
        assertThat(item1.version(), is(Versions.MATCH_ANY));
        assertThat(item1.retryOnConflict(), is(Constants.UPDATE_RETRY_ON_CONFLICT));

        ShardUpsertRequest.Item item2 = request2.items().get(1);
        assertThat(item2.id(), is("42"));
        assertThat(item2.updateAssignments(), is(new Symbol[]{Literal.newLiteral(42), Literal.newLiteral("Deep Thought") }));
        assertNull(item2.insertValues());
        assertThat(item2.routing(), is("42"));
        assertThat(item2.version(), is(2L));
        assertThat(item2.retryOnConflict(), is(0));
    }

}
