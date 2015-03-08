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
import io.crate.core.collections.RowN;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.TableIdent;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.IntegerType;
import io.crate.types.StringType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.index.shard.ShardId;
import org.junit.Test;

import java.util.*;

import static org.hamcrest.core.Is.is;

public class ShardUpsertRequestTest extends CrateUnitTest {

    TableIdent charactersIdent = new TableIdent(null, "characters");

    Reference idRef = new Reference(new ReferenceInfo(
            new ReferenceIdent(charactersIdent, "id"), RowGranularity.DOC, DataTypes.INTEGER));
    Reference nameRef = new Reference(new ReferenceInfo(
            new ReferenceIdent(charactersIdent, "name"), RowGranularity.DOC, DataTypes.STRING));

    @Test
    public void testStreamingOfInsert() throws Exception {
        ShardId shardId = new ShardId("test", 1);
        DataType[] dataTypes = new DataType[]{ IntegerType.INSTANCE, StringType.INSTANCE };
        List<Integer> rowIndicesToStream = new ArrayList();
        rowIndicesToStream.add(0);
        rowIndicesToStream.add(1);
        Map<Reference, Symbol> insertAssignments = new HashMap<Reference, Symbol>(){{
            put(idRef, new InputColumn(0));
            put(nameRef, new InputColumn(1));
        }};
        ShardUpsertRequest request = new ShardUpsertRequest(
                shardId,
                dataTypes,
                rowIndicesToStream,
                null,
                insertAssignments);

        request.add(123, "99",
                new RowN(new Object[]{99, new BytesRef("Marvin")}),
                null, "99");

        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);

        BytesStreamInput in = new BytesStreamInput(out.bytes());
        ShardUpsertRequest request2 = new ShardUpsertRequest();
        request2.readFrom(in);

        assertThat(request2.index(), is(shardId.getIndex()));
        assertThat(request2.shardId(), is(shardId.id()));
        assertThat(request2.routing(), is("99"));
        assertNull(request2.updateAssignments());
        assertThat(request2.insertAssignments(), is(insertAssignments));

        assertThat(request2.locations().size(), is(1));
        assertThat(request2.locations().get(0), is(123));

        Iterator<ShardUpsertRequest.Item> it = request2.iterator();
        while (it.hasNext()) {
            ShardUpsertRequest.Item item = it.next();
            assertThat(item.id(), is("99"));
            assertThat(item.row().size(), is(2));
            assertThat((Integer)item.row().get(0), is(99));
            assertThat((BytesRef)item.row().get(1), is(new BytesRef("Marvin")));
            assertThat(item.version(), is(Versions.MATCH_ANY));
            assertThat(item.retryOnConflict(), is(Constants.UPDATE_RETRY_ON_CONFLICT));
        }
    }

    @Test
    public void testStreamingOfUpdate() throws Exception {
        ShardId shardId = new ShardId("test", 1);
        DataType[] dataTypes = new DataType[]{ StringType.INSTANCE };
        List<Integer> rowIndicesToStream = new ArrayList();
        rowIndicesToStream.add(0);
        Map<Reference, Symbol> updateAssignments = new HashMap<Reference, Symbol>(){{ put(nameRef, new InputColumn(1)); }};

        ShardUpsertRequest request = new ShardUpsertRequest(
                shardId,
                dataTypes,
                rowIndicesToStream,
                updateAssignments,
                null,
                "99");

        request.add(123, "99",
                new RowN(new Object[]{ new BytesRef("Marvin") }),
                2L, null);


        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);

        BytesStreamInput in = new BytesStreamInput(out.bytes());
        ShardUpsertRequest request2 = new ShardUpsertRequest();
        request2.readFrom(in);

        assertThat(request2.index(), is(shardId.getIndex()));
        assertThat(request2.shardId(), is(shardId.id()));
        assertThat(request2.routing(), is("99"));
        assertThat(request2.updateAssignments(), is(updateAssignments));
        assertNull(request2.insertAssignments());

        assertThat(request2.locations().size(), is(1));
        assertThat(request2.locations().get(0), is(123));

        Iterator<ShardUpsertRequest.Item> it = request2.iterator();
        while (it.hasNext()) {
            ShardUpsertRequest.Item item = it.next();
            assertThat(item.id(), is("99"));
            assertThat(item.row().size(), is(1));
            assertThat((BytesRef)item.row().get(0), is(new BytesRef("Marvin")));
            assertThat(item.version(), is(2L));
            assertThat(item.retryOnConflict(), is(0));
        }
    }

    @Test
    public void testStreamingOfUpsert() throws Exception {
        ShardId shardId = new ShardId("test", 1);
        DataType[] dataTypes = new DataType[]{ IntegerType.INSTANCE, StringType.INSTANCE };
        List<Integer> rowIndicesToStream = new ArrayList();
        rowIndicesToStream.add(0);
        rowIndicesToStream.add(1);
        Map<Reference, Symbol> updateAssignments = new HashMap<Reference, Symbol>(){{
            put(nameRef, new InputColumn(1));
        }};
        Map<Reference, Symbol> insertAssignments = new HashMap<Reference, Symbol>(){{
            put(idRef, new InputColumn(0));
            put(nameRef, new InputColumn(1));
        }};
        ShardUpsertRequest request = new ShardUpsertRequest(
                shardId,
                dataTypes,
                rowIndicesToStream,
                updateAssignments,
                insertAssignments,
                "99");

        request.add(123, "99",
                new RowN(new Object[]{ 99, new BytesRef("Marvin") }),
                2L, null);


        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);

        BytesStreamInput in = new BytesStreamInput(out.bytes());
        ShardUpsertRequest request2 = new ShardUpsertRequest();
        request2.readFrom(in);

        assertThat(request2.index(), is(shardId.getIndex()));
        assertThat(request2.shardId(), is(shardId.id()));
        assertThat(request2.routing(), is("99"));
        assertThat(request2.updateAssignments(), is(updateAssignments));
        assertThat(request2.insertAssignments(), is(insertAssignments));

        assertThat(request2.locations().size(), is(1));
        assertThat(request2.locations().get(0), is(123));

        Iterator<ShardUpsertRequest.Item> it = request2.iterator();
        while (it.hasNext()) {
            ShardUpsertRequest.Item item = it.next();
            assertThat(item.id(), is("99"));
            assertThat(item.row().size(), is(2));
            assertThat((Integer)item.row().get(0), is(99));
            assertThat((BytesRef)item.row().get(1), is(new BytesRef("Marvin")));
            assertThat(item.version(), is(2L));
            assertThat(item.retryOnConflict(), is(0));
        }
    }

}
