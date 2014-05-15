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
import io.crate.types.*;
import junit.framework.TestCase;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import java.io.IOException;
import java.util.HashSet;

import static org.junit.Assert.assertArrayEquals;

public class SQLResponseTest extends TestCase {

    private XContentBuilder builder() throws IOException {
        return XContentFactory.contentBuilder(XContentType.JSON);
    }

    private String json(ToXContent x) throws IOException {
        return x.toXContent(builder(), ToXContent.EMPTY_PARAMS).string();
    }

    @Test
    public void testXContentInt() throws Exception {
        SQLResponse r = new SQLResponse();
        r.cols(new String[]{"col1", "col2"});
        r.rows(new Object[][]{new Object[]{1, 2}});
        r.rowCount(1L);
        //System.out.println(json(r));
        JSONAssert.assertEquals(
                "{\"cols\":[\"col1\",\"col2\"],\"rows\":[[1,2]],\"rowcount\":1,\"duration\":-1}",
                json(r), true);
    }

    @Test
    public void testXContentString() throws Exception {
        SQLResponse r = new SQLResponse();
        r.cols(new String[]{"some", "thing"});
        r.rows(new Object[][]{
                new Object[]{"one", "two"},
                new Object[]{"three", "four"},
        });
        //System.out.println(json(r));
        JSONAssert.assertEquals(
                "{\"cols\":[\"some\",\"thing\"],\"rows\":[[\"one\",\"two\"],[\"three\",\"four\"]],\"duration\":-1}",
                json(r), true);
    }

    @Test
    public void testXContentRowCount() throws Exception {
        SQLResponse r = new SQLResponse();
        r.cols(new String[]{"some", "thing"});
        r.rows(new Object[][]{
                new Object[]{"one", "two"},
                new Object[]{"three", "four"},
        });
        JSONAssert.assertEquals(
                "{\"cols\":[\"some\",\"thing\"],\"rows\":[[\"one\",\"two\"],[\"three\",\"four\"]],\"duration\":-1}",
                json(r), true);

        r.rowCount(2L);
        JSONAssert.assertEquals(
                "{\"cols\":[\"some\",\"thing\"],\"rows\":[[\"one\",\"two\"],[\"three\",\"four\"]],\"rowcount\":2,\"duration\":-1}",
                json(r), true);
    }

    @Test
    public void testXContentColumnTypes() throws Exception {
        SQLResponse r = new SQLResponse();
        r.cols(new String[]{"col1", "col2", "col3"});
        r.colTypes(new DataType[]{StringType.INSTANCE, new ArrayType(IntegerType.INSTANCE),
                new SetType(new ArrayType(LongType.INSTANCE))});
        r.includeTypes(true);
        r.rows(new Object[][]{new Object[]{1, new Integer[]{42}, new HashSet<Long[]>(){{add(new Long[]{21L});}}}});
        r.rowCount(1L);
        System.out.println(json(r));
        JSONAssert.assertEquals(
                "{\"cols\":[\"col1\",\"col2\",\"col3\"],\"col_types\":[4,[100,9],[101,[100,10]]],\"rows\":[[1,[42],[[21]]]],\"rowcount\":1,\"duration\":-1}",
                json(r), true);

    }

    @Test
    public void testResponseStreamable() throws Exception {

        BytesStreamOutput o = new BytesStreamOutput();
        SQLResponse r1, r2;

        r1 = new SQLResponse();
        r1.cols(new String[]{});
        r1.colTypes(new DataType[]{});
        r1.rows(new Object[][]{new String[]{}});

        r1.writeTo(o);

        r2 = new SQLResponse();
        r2.readFrom(new BytesStreamInput(o.bytes()));


        assertArrayEquals(r1.cols(), r2.cols());
        assertArrayEquals(r1.columnTypes(), r2.columnTypes());
        assertArrayEquals(r1.rows(), r2.rows());
        assertEquals(r1.rowCount(), r2.rowCount());

        o.reset();
        r1 = new SQLResponse();
        r1.cols(new String[]{"a", "b"});
        r1.colTypes(new DataType[]{StringType.INSTANCE, StringType.INSTANCE});
        r1.includeTypes(true);
        r1.rows(new Object[][]{new String[]{"va", "vb"}});

        r1.writeTo(o);

        r2 = new SQLResponse();
        r2.readFrom(new BytesStreamInput(o.bytes()));

        assertArrayEquals(r1.cols(), r2.cols());
        assertArrayEquals(r1.columnTypes(), r2.columnTypes());
        assertArrayEquals(r1.rows(), r2.rows());
        assertEquals(r1.rowCount(), r2.rowCount());

        o.reset();

        r1 = new SQLResponse();
        r1.cols(new String[]{"a", "b"});
        r1.colTypes(new DataType[0]);
        r1.rows(new Object[][]{new String[]{"ab","ba"}, new String[]{"ba", "ab"}});
        r1.rowCount(2L);

        r1.writeTo(o);

        r2 = new SQLResponse();
        r2.readFrom(new BytesStreamInput(o.bytes()));

        assertArrayEquals(r1.cols(), r2.cols());
        assertArrayEquals(r1.columnTypes(), r2.columnTypes());
        assertArrayEquals(r1.rows(), r2.rows());
        assertEquals(r1.rowCount(), r2.rowCount());
    }

}
