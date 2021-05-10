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

package io.crate.rest.action;

import io.crate.breaker.RamAccounting;
import io.crate.breaker.RowAccountingWithEstimators;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowN;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import org.elasticsearch.test.ESTestCase;
import io.crate.types.DataTypes;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class RestActionReceiversTest extends ESTestCase {

    private final List<RowN> rows = List.of(
        new RowN("foo", 1, true),
        new RowN("bar", 2, false),
        new RowN("foobar", 3, null)
    );
    private final List<Symbol> fields = List.of(
        new ScopedSymbol(new RelationName("doc", "dummy"), ColumnIdent.fromPath("doc.col_a"), DataTypes.STRING),
        new ScopedSymbol(new RelationName("doc", "dummy"), ColumnIdent.fromPath("doc.col_b"), DataTypes.INTEGER),
        new ScopedSymbol(new RelationName("doc", "dummy"), ColumnIdent.fromPath("doc.col_c"), DataTypes.BOOLEAN)
    );
    private final Row row = new Row1(1L);

    private static void assertXContentBuilder(XContentBuilder expected, XContentBuilder actual) throws IOException {
        assertEquals(
            stripDuration(Strings.toString(expected)),
            stripDuration(Strings.toString(actual))
        );
    }

    private static String stripDuration(String s) {
        return s.replaceAll(",\"duration\":[^,}]+", "");
    }

    @Test
    public void testRestRowCountReceiver() throws Exception {
        RestRowCountReceiver receiver = new RestRowCountReceiver(JsonXContent.contentBuilder(), 0L, true);
        receiver.setNextRow(row);
        XContentBuilder actualBuilder = receiver.finishBuilder();

        ResultToXContentBuilder builder = ResultToXContentBuilder.builder(JsonXContent.contentBuilder());
        builder.cols(Collections.<ScopedSymbol>emptyList());
        builder.colTypes(Collections.<ScopedSymbol>emptyList());
        builder.startRows();
        builder.addRow(row, 0);
        builder.finishRows();
        builder.rowCount(1L);

        assertXContentBuilder(actualBuilder, builder.build());
    }

    @Test
    public void testRestResultSetReceiver() throws Exception {
        RestResultSetReceiver receiver = new RestResultSetReceiver(
            JsonXContent.contentBuilder(),
            fields,
            0L,
            new RowAccountingWithEstimators(Symbols.typeView(fields), RamAccounting.NO_ACCOUNTING),
            true
        );
        for (Row row : rows) {
            receiver.setNextRow(row);
        }
        XContentBuilder actualBuilder = receiver.finishBuilder();

        ResultToXContentBuilder builder = ResultToXContentBuilder.builder(JsonXContent.contentBuilder());
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
        ResultToXContentBuilder builder = ResultToXContentBuilder.builder(JsonXContent.contentBuilder())
            .bulkRows(results);
        String s = Strings.toString(builder.build());
        assertEquals(s, "{\"results\":[{\"rowcount\":1},{\"rowcount\":2},{\"rowcount\":3}]}");
    }
}
