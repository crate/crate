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

package io.crate.execution.engine.collect;

import static io.crate.testing.TestingHelpers.createNodeContext;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.SysColumns;

public class RowShardResolverTest extends ESTestCase {

    private static final ColumnIdent ID_IDENT = ColumnIdent.of("_id");

    private Row row(Object... cells) {
        if (cells == null) {
            cells = new Object[]{null};
        }
        return new RowN(cells);
    }

    private ColumnIdent ci(String ident) {
        return ColumnIdent.of(ident);
    }

    private final TransactionContext txnCtx = CoordinatorTxnCtx.systemTransactionContext();
    private final NodeContext nodeCtx = createNodeContext();

    @Test
    public void testNoPrimaryKeyNoRouting() {
        RowShardResolver rowShardResolver =
            new RowShardResolver(txnCtx, nodeCtx, List.of(), List.of(), null, null);
        rowShardResolver.setNextRow(row());

        // auto-generated id, no special routing
        assertThat(rowShardResolver.id()).isNotNull();
        assertThat(rowShardResolver.routing()).isNull();
    }

    @Test
    public void testNoPrimaryKeyButRouting() {
        RowShardResolver rowShardResolver =
            new RowShardResolver(txnCtx, nodeCtx, List.of(), List.of(), ID_IDENT, new InputColumn(1));
        rowShardResolver.setNextRow(row(1, "hoschi"));

        // auto-generated id, special routing
        assertThat(rowShardResolver.id()).isNotNull();
        assertThat(rowShardResolver.routing()).isEqualTo("hoschi");
    }

    @Test
    public void testPrimaryKeyNoRouting() {
        List<Symbol> primaryKeySymbols = List.of(new InputColumn(0), new InputColumn(1));
        RowShardResolver rowShardResolver =
            new RowShardResolver(txnCtx, nodeCtx, List.of(ci("id"), ci("foo")), primaryKeySymbols, null, null);
        rowShardResolver.setNextRow(row(1, "hoschi"));

        // compound encoded id, no special routing
        assertThat(rowShardResolver.id()).isEqualTo("AgExBmhvc2NoaQ==");
        assertThat(rowShardResolver.routing()).isNull();
    }

    @Test
    public void testPrimaryKeyAndRouting() {
        List<Symbol> primaryKeySymbols = List.of(new InputColumn(0), new InputColumn(1));
        RowShardResolver rowShardResolver =
            new RowShardResolver(txnCtx, nodeCtx, List.of(ci("id"), ci("foo")), primaryKeySymbols, ci("foo"), new InputColumn(1));
        rowShardResolver.setNextRow(row(1, "hoschi"));

        // compound encoded id, special routing
        assertThat(rowShardResolver.id()).isEqualTo("AgZob3NjaGkBMQ==");
        assertThat(rowShardResolver.routing()).isEqualTo("hoschi");
    }

    @Test
    public void testMultipleRows() {
        List<Symbol> primaryKeySymbols = List.of(new InputColumn(0), new InputColumn(1));
        RowShardResolver rowShardResolver =
            new RowShardResolver(txnCtx, nodeCtx, List.of(ci("id"), ci("foo")), primaryKeySymbols, ci("foo"), new InputColumn(1));

        rowShardResolver.setNextRow(row(1, "hoschi"));
        assertThat(rowShardResolver.id()).isEqualTo("AgZob3NjaGkBMQ==");
        assertThat(rowShardResolver.routing()).isEqualTo("hoschi");

        rowShardResolver.setNextRow(row(2, "galoschi"));
        assertThat(rowShardResolver.id()).isEqualTo("AghnYWxvc2NoaQEy");
        assertThat(rowShardResolver.routing()).isEqualTo("galoschi");
    }

    @Test
    public void testIdPrimaryKeyNull() {
        List<Symbol> primaryKeySymbols = List.of(new InputColumn(2));
        RowShardResolver rowShardResolver =
            new RowShardResolver(txnCtx, nodeCtx, List.of(ID_IDENT), primaryKeySymbols, null, new InputColumn(1));
        rowShardResolver.setNextRow(row(1, "hoschi", null));

        // generated _id, special routing
        assertThat(rowShardResolver.id()).isNotNull();
        assertThat(rowShardResolver.routing()).isEqualTo("hoschi");
    }

    @Test
    public void testPrimaryKeyNullException() {
        List<Symbol> primaryKeySymbols = List.of(new InputColumn(0));
        RowShardResolver rowShardResolver =
            new RowShardResolver(txnCtx, nodeCtx, List.of(ci("id")), primaryKeySymbols, null, null);

        assertThatThrownBy(() -> rowShardResolver.setNextRow(row(new Object[]{null})))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("A primary key value must not be NULL");
    }

    @Test
    public void testMultiPrimaryKeyNullException() {
        List<Symbol> primaryKeySymbols = List.of(new InputColumn(1), new InputColumn(0));
        RowShardResolver rowShardResolver =
            new RowShardResolver(txnCtx, nodeCtx, List.of(ci("id"), ci("foo")), primaryKeySymbols, null, new InputColumn(1));

        assertThatThrownBy(() -> rowShardResolver.setNextRow(row(1, null)))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("A primary key value must not be NULL");
    }

    @Test
    public void test_auto_generated_timestamp_is_set_if_pk_is_internal_id() {
        List<Symbol> primaryKeySymbols = List.of(new InputColumn(0));
        var rowShardResolver = new RowShardResolver(
            txnCtx,
            nodeCtx,
            List.of(SysColumns.ID.COLUMN),
            primaryKeySymbols,
            null,
            null
        );
        rowShardResolver.setNextRow(row(1));

        assertThat(rowShardResolver.autoGeneratedTimestamp()).isGreaterThan(Translog.UNSET_AUTO_GENERATED_TIMESTAMP);
    }

    @Test
    public void test_auto_generated_timestamp_is_not_set_if_pk_is_not_internal_id() {
        List<Symbol> primaryKeySymbols = List.of(new InputColumn(0));
        var rowShardResolver = new RowShardResolver(
            txnCtx,
            nodeCtx,
            List.of(ColumnIdent.of("my_pk")),
            primaryKeySymbols,
            null,
            null
        );
        rowShardResolver.setNextRow(row(1));
        assertThat(rowShardResolver.autoGeneratedTimestamp()).isEqualTo(Translog.UNSET_AUTO_GENERATED_TIMESTAMP);
    }

    @Test
    public void test_auto_generated_timestamp_is_not_set_if_multiple_pk_are_used() {
        List<Symbol> primaryKeySymbols = List.of(new InputColumn(0), new InputColumn(1));
        var rowShardResolver = new RowShardResolver(
            txnCtx,
            nodeCtx,
            List.of(SysColumns.ID.COLUMN, ColumnIdent.of("my_pk")),
            primaryKeySymbols,
            null,
            null
        );
        rowShardResolver.setNextRow(row(1, "hoschi"));
        assertThat(rowShardResolver.autoGeneratedTimestamp()).isEqualTo(Translog.UNSET_AUTO_GENERATED_TIMESTAMP);
    }

    /**
     * Test partition table scenario, were no primary key is defined.
     * See {@link io.crate.metadata.doc.DocTableInfo}
     */
    @Test
    public void test_auto_generated_timestamp_is_set_if_no_pk_are_used() {
        List<Symbol> primaryKeySymbols = List.of(new InputColumn(0));
        var rowShardResolver = new RowShardResolver(
            txnCtx,
            nodeCtx,
            List.of(),
            primaryKeySymbols,
            null,
            null
        );
        rowShardResolver.setNextRow(row(1));
        assertThat(rowShardResolver.autoGeneratedTimestamp()).isGreaterThan(Translog.UNSET_AUTO_GENERATED_TIMESTAMP);
    }
}
