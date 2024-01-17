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

package io.crate.analyze;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;

import org.junit.Test;

import io.crate.common.collections.Lists;
import io.crate.data.Row;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.planner.operators.SubQueryResults;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataType;

public class CreateTableAsAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testSimpleCompareAgainstAnalyzedCreateTable() throws IOException {

        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable(
                "create table tbl (" +
                "   col_default_object object as (" +
                "       col_nested_integer integer," +
                "       col_nested_object object as (" +
                "           col_nested_timestamp_with_time_zone timestamp with time zone" +
                "       )" +
                "   )" +
                ")"
            )
            .build();

        var expected = ((AnalyzedCreateTable) e.analyze(
            "create table cpy (" +
            "   col_default_object object as (" +
            "       col_nested_integer integer," +
            "       col_nested_object object as (" +
            "           col_nested_timestamp_with_time_zone timestamp with time zone" +
            "       )" +
            "   )" +
            ")"
        )).bind(
            new NumberOfShards(clusterService),
            e.fulltextAnalyzerResolver(),
            e.nodeCtx,
            CoordinatorTxnCtx.systemTransactionContext(),
            Row.EMPTY,
            SubQueryResults.EMPTY
        );

        AnalyzedCreateTableAs analyzedCreateTableAs = e.analyze(
            "create table cpy as select * from  tbl"
        );
        var actual = analyzedCreateTableAs.analyzedCreateTable().bind(
            new NumberOfShards(clusterService),
            e.fulltextAnalyzerResolver(),
            e.nodeCtx,
            CoordinatorTxnCtx.systemTransactionContext(),
            Row.EMPTY,
            SubQueryResults.EMPTY
        );

        assertThat(expected.tableName()).isEqualTo(actual.tableName());
        assertThat(expected.columns().keySet()).containsExactlyElementsOf(actual.columns().keySet());
        List<DataType<?>> expectedTypes = Lists.map(expected.columns().values(), Symbol::valueType);
        List<DataType<?>> actualTypes = Lists.map(actual.columns().values(), Symbol::valueType);
        assertThat(expectedTypes).containsExactlyElementsOf(actualTypes);
    }
}
