/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.module.sql.test;

import io.crate.action.sql.SQLResponse;
import io.crate.core.collections.Buckets;
import io.crate.core.collections.CollectionBucket;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.*;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.UUID;

import static org.hamcrest.core.Is.is;

public class SQLResponseTest extends CrateUnitTest {

    private XContentBuilder builder() throws IOException {
        return XContentFactory.contentBuilder(XContentType.JSON);
    }

    private String json(ToXContent x) throws IOException {
        return x.toXContent(builder(), ToXContent.EMPTY_PARAMS).string();
    }

    @Test
    public void testXContentInt() throws Exception {
        SQLResponse r = new SQLResponse(
            new String[]{"col1", "col2"},
            new CollectionBucket(Arrays.<Object[]>asList(new Object[] {1, 2})),
            new DataType[] { DataTypes.INTEGER, DataTypes.INTEGER },
            1L,
            0.0f,
            UUID.randomUUID()
        );
        JSONAssert.assertEquals(
                "{\"cols\":[\"col1\",\"col2\"],\"rows\":[[1,2]],\"rowcount\":1,\"duration\":0, \"col_types\": [9, 9]}}",
                json(r), true);
    }

    @Test
    public void testXContentString() throws Exception {
        SQLResponse r = new SQLResponse(
            new String[]{"some", "thing"},
            new CollectionBucket(Arrays.asList(
                new Object[]{"one", "two"},
                new Object[]{"three", "four"}
            )),
            new DataType[] { DataTypes.STRING, DataTypes.STRING },
            2L,
            0.0f,
            UUID.randomUUID()
        );
        JSONAssert.assertEquals(
                "{\"cols\":[\"some\",\"thing\"],\"rows\":[[\"one\",\"two\"],[\"three\",\"four\"]],\"rowcount\": 2, \"duration\":0, \"col_types\": [4, 4]}",
                json(r), true);
    }

    @Test
    public void testXContentRowCount() throws Exception {
        SQLResponse r = new SQLResponse(
            new String[]{"some", "thing"},
            new CollectionBucket(Arrays.asList(
                new Object[]{"one", "two"},
                new Object[]{"three", "four"}
            )),
            new DataType[] { DataTypes.STRING, DataTypes.STRING },
            2L,
            0.0f,
            UUID.randomUUID()
        );
        JSONAssert.assertEquals(
                "{\"cols\":[\"some\",\"thing\"],\"rows\":[[\"one\",\"two\"],[\"three\",\"four\"]],\"rowcount\":2,\"duration\":0, \"col_types\": [4, 4]}",
                json(r), true);
    }

    @Test
    public void testXContentColumnTypes() throws Exception {
        SQLResponse r = new SQLResponse(
            new String[]{"col1", "col2", "col3"},
            new CollectionBucket(Collections.singletonList(
                new Object[]{1, new Integer[]{42}, new HashSet<Long[]>(){{add(new Long[]{21L});}}}
            )),
            new DataType[]{StringType.INSTANCE, new ArrayType(IntegerType.INSTANCE),
                new SetType(new ArrayType(LongType.INSTANCE))},
            1L,
            0.0f,
            UUID.randomUUID()
        );
        System.out.println(json(r));
        JSONAssert.assertEquals(
                "{\"cols\":[\"col1\",\"col2\",\"col3\"],\"col_types\":[4,[100,9],[101,[100,10]]],\"rows\":[[1,[42],[[21]]]],\"rowcount\":1,\"duration\":0}",
                json(r), true);

    }

    @Test
    public void testResponseStreamable() throws Exception {

        BytesStreamOutput o = new BytesStreamOutput();
        SQLResponse r1, r2;

        r1 = new SQLResponse(
            new String[] {},
            new CollectionBucket(Collections.<Object[]>singletonList(new String[0])),
            new DataType[] {},
            0L,
            10.f,
            new UUID(20L, 24L));

        r1.writeTo(o);

        r2 = new SQLResponse();
        r2.readFrom(StreamInput.wrap(o.bytes()));


        assertArrayEquals(r1.cols(), r2.cols());
        assertArrayEquals(r1.columnTypes(), r2.columnTypes());
        assertThat(Buckets.materialize(r2.bucket()), is(Buckets.materialize(r1.bucket())));
        assertEquals(r1.rowCount(), r2.rowCount());
        assertThat(r1.duration(), is(r2.duration()));

        o.reset();
        r1 = new SQLResponse(
            new String[] {"a", "b"},
            new CollectionBucket(Arrays.<Object[]>asList(
                new Object[]{new BytesRef("va"), new BytesRef("vb")}
            )),
            new DataType[] {DataTypes.STRING, DataTypes.STRING},
            2L,
            10.f,
            new UUID(20L, 24L));

        r1.writeTo(o);

        r2 = new SQLResponse();
        r2.readFrom(StreamInput.wrap(o.bytes()));

        assertArrayEquals(r1.cols(), r2.cols());
        assertArrayEquals(r1.columnTypes(), r2.columnTypes());
        assertArrayEquals(r1.rows(), r2.rows());
        assertEquals(r1.rowCount(), r2.rowCount());

        o.reset();
    }


    /**
     * the serialization tests here with fixed bytes arrays ensure that backward-compatibility isn't broken.
     */

    @Test
    public void testSerializationWriteTo() throws Exception {
        SQLResponse resp = new SQLResponse(
            new String[] {"col1", "col2"},
            new CollectionBucket(Arrays.asList(
                    new Object[] {new BytesRef("row1_col1"), new BytesRef("row1_col2")},
                    new Object[] {new BytesRef("row2_col1"), new BytesRef("row2_col2")}
            )),
            new DataType[] { DataTypes.STRING, DataTypes.STRING },
            2L,
            0,
            new UUID(20L, 24L)
        );

        BytesStreamOutput out = new BytesStreamOutput();
        resp.writeTo(out);

        byte[] expectedBytes = new byte[]
            {0, 2, 4, 99, 111, 108, 49, 4, 99, 111, 108, 50, 1, 0, 0, 0, 0, 2, 4, 4, 0, 0, 0, 0, 0, 0, 0, 20, 0, 0, 0, 0, 0, 0, 0, 24, 0, 2, 2, 40, 10, 114, 111, 119, 49, 95, 99, 111, 108, 49, 10, 114, 111, 119, 49, 95, 99, 111, 108, 50, 10, 114, 111, 119, 50, 95, 99, 111, 108, 49, 10, 114, 111, 119, 50, 95, 99, 111, 108, 50};
        byte[] bytes = out.bytes().toBytes();
        assertThat(bytes, is(expectedBytes));
    }

    @Test
    public void testSerializationReadFrom() throws Exception {
        byte[] buf = new byte[]
            {0, 2, 4, 99, 111, 108, 49, 4, 99, 111, 108, 50, 1, 0, 0, 0, 0, 2, 4, 4, 0, 0, 0, 0, 0, 0, 0, 20, 0, 0, 0, 0, 0, 0, 0, 24, 0, 2, 2, 40, 10, 114, 111, 119, 49, 95, 99, 111, 108, 49, 10, 114, 111, 119, 49, 95, 99, 111, 108, 50, 10, 114, 111, 119, 50, 95, 99, 111, 108, 49, 10, 114, 111, 119, 50, 95, 99, 111, 108, 50};
        StreamInput in = StreamInput.wrap(buf);
        SQLResponse resp = new SQLResponse();
        resp.readFrom(in);

        assertThat(resp.cols(), is(new String[] { "col1", "col2" }));
        assertThat(resp.rows(), is(new Object[][] {
                new Object[] {"row1_col1", "row1_col2"},
                new Object[] {"row2_col1", "row2_col2"},
        }));

        assertThat(resp.columnTypes(), is(new DataType[] { DataTypes.STRING, DataTypes.STRING }));
        assertThat(resp.rowCount(), is(2L));
        assertThat(resp.duration(), is(0f));
        assertThat(resp.cursorId(), is(new UUID(20L, 24L)));
    }
}
