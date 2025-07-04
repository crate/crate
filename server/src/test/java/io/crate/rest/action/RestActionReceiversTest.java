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

import static io.crate.data.breaker.BlockBasedRamAccounting.MAX_BLOCK_SIZE_IN_BYTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.TestCircuitBreaker;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.auth.AccessControl;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowN;
import io.crate.data.breaker.BlockBasedRamAccounting;
import io.crate.data.breaker.RamAccounting;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.session.ResultReceiver;
import io.crate.types.DataTypes;

public class RestActionReceiversTest extends ESTestCase {

    private final List<RowN> rows = List.of(
        new RowN("foo", 1, true),
        new RowN("bar", 2, false),
        new RowN("abc", Long.MAX_VALUE, false)
    );
    private final List<Symbol> fields = List.of(
        new ScopedSymbol(new RelationName("doc", "dummy"), ColumnIdent.fromPath("doc.col_a"), DataTypes.STRING),
        new ScopedSymbol(new RelationName("doc", "dummy"), ColumnIdent.fromPath("doc.col_b"), DataTypes.LONG),
        new ScopedSymbol(new RelationName("doc", "dummy"), ColumnIdent.fromPath("doc.col_c"), DataTypes.BOOLEAN)
    );
    private final List<String> fieldNames = List.of("col_a", "col_b", "col_c");
    private final Row row = new Row1(1L);

    private static void assertXContentBuilder(XContentBuilder expected, XContentBuilder actual) throws IOException {
        assertThat(stripDuration(Strings.toString(actual))
        ).isEqualTo(
            stripDuration(Strings.toString(expected)));
    }

    private static String stripDuration(String s) {
        return s.replaceAll(",\"duration\":[^,}]+", "");
    }

    @Test
    public void testRestRowCountReceiver() throws Exception {
        RestRowCountReceiver receiver = new RestRowCountReceiver(JsonXContent.builder(), 0L, true);
        receiver.setNextRow(row);
        XContentBuilder actualBuilder = receiver.finishBuilder();

        ResultToXContentBuilder builder = ResultToXContentBuilder.builder(JsonXContent.builder());
        builder.cols(Collections.emptyList());
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
            JsonXContent.builder(),
            fields,
            fieldNames,
            0L,
            true
        );
        for (Row row : rows) {
            receiver.setNextRow(row);
        }
        XContentBuilder actualBuilder = receiver.finishBuilder();

        ResultToXContentBuilder builder = ResultToXContentBuilder.builder(JsonXContent.builder());
        builder.cols(fieldNames);
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
            new RestBulkRowCountReceiver.Result(1, null),
            new RestBulkRowCountReceiver.Result(-2, new IllegalArgumentException("invalid input")),
            new RestBulkRowCountReceiver.Result(-2, new VersionConflictEngineException(new ShardId("dummy", "dummy", 1), "document already exists", null)),
        };
        ResultToXContentBuilder builder = ResultToXContentBuilder.builder(JsonXContent.builder())
            .bulkRows(results, AccessControl.DISABLED);
        String s = Strings.toString(builder.build());
        assertThat("{\"results\":[" +
            "{\"rowcount\":1}," +
            "{\"rowcount\":-2,\"error\":{\"code\":4000,\"message\":\"SQLParseException[invalid input]\"}}," +
            "{\"rowcount\":-2,\"error\":{\"code\":4091,\"message\":\"DuplicateKeyException[A document with the same primary key exists already]\"}}" +
            "]}").isEqualTo(s);
    }

    @Test
    public void test_rest_bulk_row_count_receiver_supports_single_column_row_on_single_bulk_arg() throws Exception {
        var results = new RestBulkRowCountReceiver.Result[1];
        var bulkRowCountReceiver = new RestBulkRowCountReceiver(results, 0);
        // A row with a single column must not throw an exception on reading a possible second column
        bulkRowCountReceiver.setNextRow(new Row1(1L));
        bulkRowCountReceiver.allFinished();
        assertThat(results[0].rowCount()).isEqualTo(1L);
        assertThat(results[0].error()).isNull();
    }

    @Test
    public void test_result_receiver_future_is_not_completed_on_cbe() throws Exception {
        TestCircuitBreaker breaker = new TestCircuitBreaker();
        breaker.startBreaking();
        RamAccounting ramAccounting = new BlockBasedRamAccounting(
            b -> breaker.addEstimateBytesAndMaybeBreak(b, "http-result"),
            MAX_BLOCK_SIZE_IN_BYTES);
        XContentBuilder jsonXContentBuilder = new XContentBuilder(
            JsonXContent.JSON_XCONTENT, new SqlHttpHandler.RamAccountingOutputStream(ramAccounting));
        ResultReceiver<XContentBuilder> resultReceiver = new RestResultSetReceiver(
            jsonXContentBuilder,
            fields,
            fieldNames,
            System.currentTimeMillis(),
            false
        );

        // Fails with CBE, resultReceiver's future must not be completed,
        // it's handled by the consumer/response emitter which also closes iterator/clears sys.jobs entry
        assertThatThrownBy(() -> {
            resultReceiver.setNextRow(rows.get(0));
            jsonXContentBuilder.flush(); // flush the internal buffer to OutputStream to trigger ram-accounting
        })
            .isExactlyInstanceOf(CircuitBreakingException.class);
        assertThat(resultReceiver.completionFuture().isDone()).isFalse();
    }

    @Test
    public void test_ram_accounting_of_the_result_set_receiver() throws Exception {
        RamAccounting ramAccounting = new BlockBasedRamAccounting(
            b -> new TestCircuitBreaker().addEstimateBytesAndMaybeBreak(b, "http-result"),
            MAX_BLOCK_SIZE_IN_BYTES);
        SqlHttpHandler.RamAccountingOutputStream ramAccountingOutputStream = new SqlHttpHandler.RamAccountingOutputStream(ramAccounting);
        XContentBuilder jsonXContentBuilder = new XContentBuilder(JsonXContent.JSON_XCONTENT, ramAccountingOutputStream);
        ResultReceiver<XContentBuilder> resultReceiver = new RestResultSetReceiver(
            jsonXContentBuilder,
            fields,
            fieldNames,
            System.currentTimeMillis(),
            false
        );

        resultReceiver.setNextRow(rows.get(0));
        jsonXContentBuilder.flush(); // flush the internal buffer to OutputStream to trigger ram-accounting
        long bytesFirstRow = ramAccounting.totalBytes();

        resultReceiver.setNextRow(rows.get(1));
        jsonXContentBuilder.flush();
        long bytesSecondRow = ramAccounting.totalBytes() - bytesFirstRow;

        resultReceiver.setNextRow(rows.get(2));
        jsonXContentBuilder.flush();
        long bytesThirdRow = ramAccounting.totalBytes() - bytesSecondRow - bytesFirstRow;

        assertThat(bytesThirdRow - bytesSecondRow).isEqualTo(
            String.valueOf(Long.MAX_VALUE).length() - String.valueOf(2L).length()
        );
    }
}
