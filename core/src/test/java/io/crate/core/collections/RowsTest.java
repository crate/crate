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

package io.crate.core.collections;

import com.google.common.collect.Iterables;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.is;

public class RowsTest extends CrateUnitTest{

    @Test
    public void testSerialization() throws Exception {
        Rows rows = new Rows(new DataType[]{
                DataTypes.LONG,
                DataTypes.STRING,
                DataTypes.STRING
        }, Arrays.asList(0, 1, 2));
        for (int i = 0; i < randomInt(100); i++) {
            rows.addSafe(new RowN(new Object[]{
                    randomLong(),
                    new BytesRef(randomUnicodeOfLength(5)),
                    new BytesRef(randomUnicodeOfLength(5))
                }
            ));
        }

        Rows toWrite = rows;
        for (int i = 0; i < 3; i++) {
            BytesStreamOutput out = new BytesStreamOutput();
            toWrite.writeTo(out);
            BytesStreamInput in = new BytesStreamInput(out.bytes());
            Rows streamedRows = new Rows();
            streamedRows.readFrom(in);

            List<Row> rowList = new ArrayList<>();
            Iterables.addAll(rowList, toWrite);

            List<Row> streamedRowList = new ArrayList<>();
            Iterables.addAll(streamedRowList, streamedRows);
            assertThat(streamedRowList.size(), is(rowList.size()));
            for (int j = 0; j < streamedRowList.size(); j++) {
                assertThat(streamedRowList.get(j).materialize(), is(rowList.get(j).materialize()));
            }
            toWrite = streamedRows;
        }


    }

    @Test
    public void testSelectiveSerialization() throws Exception {
        Rows rows = new Rows(new DataType[]{
                DataTypes.INTEGER,
                DataTypes.STRING
        }, Arrays.asList(3, 2));
        for (int i = 0; i < randomInt(100); i++) {
            rows.addSafe(new RowN(new Object[]{
                    randomLong(),
                    randomByte(),
                    randomInt(),
                    BytesRefs.toBytesRef(i)
            }
            ));
        }

        Rows toWrite = rows;
        for (int i = 0; i < 3; i++) {
            BytesStreamOutput out = new BytesStreamOutput();
            toWrite.writeTo(out);
            BytesStreamInput in = new BytesStreamInput(out.bytes());
            Rows streamedRows = new Rows();
            streamedRows.readFrom(in);

            List<Row> rowList = new ArrayList<>();
            Iterables.addAll(rowList, toWrite);

            List<Row> streamedRowList = new ArrayList<>();
            Iterables.addAll(streamedRowList, streamedRows);
            assertThat(streamedRowList.size(), is(rowList.size()));
            for (int j = 0; j < streamedRowList.size(); j++) {
                assertThat(streamedRowList.get(j).size(), is(2)); // length is shortened
                // access by source column index works as if row is still in pre-stream-state
                assertThat(streamedRowList.get(j).get(2), is(rowList.get(j).get(2)));
                assertThat(streamedRowList.get(j).get(3), is(rowList.get(j).get(3)));
            }
            toWrite = streamedRows;
        }

    }
}
