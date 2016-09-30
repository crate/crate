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
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.*;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Test;

import static org.hamcrest.core.Is.is;

public class SQLResponseTest extends CrateUnitTest {

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
        r2.readFrom(StreamInput.wrap(o.bytes()));


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
        r2.readFrom(StreamInput.wrap(o.bytes()));

        assertArrayEquals(r1.cols(), r2.cols());
        assertArrayEquals(r1.columnTypes(), r2.columnTypes());
        assertArrayEquals(r1.rows(), r2.rows());
        assertEquals(r1.rowCount(), r2.rowCount());

        o.reset();

        r1 = new SQLResponse();
        r1.cols(new String[]{"a", "b"});
        r1.colTypes(new DataType[0]);
        r1.rows(new Object[][]{new String[]{"ab", "ba"}, new String[]{"ba", "ab"}});
        r1.rowCount(2L);

        r1.writeTo(o);

        r2 = new SQLResponse();
        r2.readFrom(StreamInput.wrap(o.bytes()));

        assertArrayEquals(r1.cols(), r2.cols());
        assertArrayEquals(r1.columnTypes(), r2.columnTypes());
        assertArrayEquals(r1.rows(), r2.rows());
        assertEquals(r1.rowCount(), r2.rowCount());
    }


    /**
     * the serialization tests here with fixed bytes arrays ensure that backward-compatibility isn't broken.
     */

    @Test
    public void testSerializationWriteTo() throws Exception {
        SQLResponse resp = new SQLResponse(
            new String[]{"col1", "col2"},
            new Object[][]{
                new Object[]{"row1_col1", "row1_col2"},
                new Object[]{"row2_col1", "row2_col2"}
            },
            new DataType[]{DataTypes.STRING, DataTypes.STRING},
            2L,
            0,
            true
        );

        BytesStreamOutput out = new BytesStreamOutput();
        resp.writeTo(out);

        byte[] expectedBytes = new byte[]
            {0, 2, 4, 99, 111, 108, 49, 4, 99, 111, 108, 50, 1, 0, 0, 0, 0, 2, 4, 4, 0, 2, 0, 0, 0, 2, 0, 9, 114, 111, 119, 49, 95, 99, 111, 108, 49, 0, 9, 114, 111, 119, 49, 95, 99, 111, 108, 50, 0, 9, 114, 111, 119, 50, 95, 99, 111, 108, 49, 0, 9, 114, 111, 119, 50, 95, 99, 111, 108, 50};
        byte[] bytes = out.bytes().toBytes();
        assertThat(bytes, is(expectedBytes));
    }

    @Test
    public void testSerializationReadFrom() throws Exception {
        byte[] buf = new byte[]
            {0, 2, 4, 99, 111, 108, 49, 4, 99, 111, 108, 50, 1, 0, 0, 0, 0, 2, 4, 4, 0, 2, 0, 0, 0, 2, 0, 9, 114, 111, 119, 49, 95, 99, 111, 108, 49, 0, 9, 114, 111, 119, 49, 95, 99, 111, 108, 50, 0, 9, 114, 111, 119, 50, 95, 99, 111, 108, 49, 0, 9, 114, 111, 119, 50, 95, 99, 111, 108, 50};
        StreamInput in = StreamInput.wrap(buf);
        SQLResponse resp = new SQLResponse();
        resp.readFrom(in);

        assertThat(resp.cols(), is(new String[]{"col1", "col2"}));
        assertThat(resp.rows(), is(new Object[][]{
            new Object[]{"row1_col1", "row1_col2"},
            new Object[]{"row2_col1", "row2_col2"},
        }));

        assertThat(resp.columnTypes(), is(new DataType[]{DataTypes.STRING, DataTypes.STRING}));
        assertThat(resp.rowCount(), is(2L));
        assertThat(resp.duration(), is(0f));
    }
}
