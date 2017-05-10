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

package io.crate.rest.action;

import com.google.common.collect.ImmutableList;
import io.crate.analyze.symbol.Field;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowN;
import io.crate.metadata.ColumnIdent;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.DummyRelation;
import io.crate.types.DataTypes;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.RestChannel;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.*;

public class RestActionReceiversTest extends CrateUnitTest {

    private final ImmutableList<RowN> rows = ImmutableList.of(
        new RowN(new Object[]{"foo", 1, true}),
        new RowN(new Object[]{"bar", 2, false}),
        new RowN(new Object[]{"foobar", 3, null})
    );
    private final List<Field> fields = ImmutableList.of(
        new Field(new DummyRelation(), ColumnIdent.fromPath("doc.col_a"), DataTypes.STRING),
        new Field(new DummyRelation(), ColumnIdent.fromPath("doc.col_b"), DataTypes.INTEGER),
        new Field(new DummyRelation(), ColumnIdent.fromPath("doc.col_c"), DataTypes.BOOLEAN)
    );
    private final Row row = new Row1(1L);

    private static void assertXContentBuilder(XContentBuilder expected, XContentBuilder actual) throws IOException {
        assertEquals(
            stripDuration(expected.string()),
            stripDuration(actual.string())
        );
    }

    private static String stripDuration(String s) {
        return s.replaceAll(",\"duration\":[^,}]+", "");
    }

    private static RestChannel newChannel() throws IOException {
        RestChannel channel = mock(RestChannel.class);
        XContentBuilder xContentBuilder = JsonXContent.contentBuilder();
        when(channel.newBuilder()).thenReturn(xContentBuilder);
        return channel;
    }

    @Test
    public void testRestRowCountReceiver() throws Exception {
        RestRowCountReceiver receiver = new RestRowCountReceiver(newChannel(), 0L, true);
        receiver.setNextRow(row);
        XContentBuilder actualBuilder = receiver.finishBuilder();

        ResultToXContentBuilder builder = ResultToXContentBuilder.builder(newChannel());
        builder.cols(Collections.<Field>emptyList());
        builder.colTypes(Collections.<Field>emptyList());
        builder.startRows();
        builder.addRow(row, 0);
        builder.finishRows();
        builder.rowCount(1L);

        assertXContentBuilder(actualBuilder, builder.build());
    }

    @Test
    public void testRestResultSetReceiver() throws Exception {
        RestResultSetReceiver receiver = new RestResultSetReceiver(newChannel(), fields, 0L, true);
        for (Row row : rows) {
            receiver.setNextRow(row);
        }
        XContentBuilder actualBuilder = receiver.finishBuilder();

        ResultToXContentBuilder builder = ResultToXContentBuilder.builder(newChannel());
        builder.cols(fields);
        builder.colTypes(fields);
        builder.startRows();
        for (Row row : rows) {
            builder.addRow(row, 3);
        }
        builder.finishRows();
        builder.rowCount(rows.size());

        assertXContentBuilder(actualBuilder, builder.build());
    }

    @Test
    public void testRestBulkRowCountReceiver() throws Exception {
        RestBulkRowCountReceiver.Result[] results = new RestBulkRowCountReceiver.Result[] {
            new RestBulkRowCountReceiver.Result(null, 1),
            new RestBulkRowCountReceiver.Result(null, 2),
            new RestBulkRowCountReceiver.Result(null, 3)
        };
        ResultToXContentBuilder builder = ResultToXContentBuilder.builder(newChannel())
            .bulkRows(results);
        String s = builder.build().string();
        assertEquals(s, "{\"results\":[{\"rowcount\":1},{\"rowcount\":2},{\"rowcount\":3}]}");
    }
}
