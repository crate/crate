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

package io.crate.fdw;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.Test;

import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class JdbcBatchIteratorTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_all_column_names_get_quoted() throws Exception {
        // Foreign SQL databases may use different keywords than CrateDB
        // → Identifiers.quoteIfNeeded() isn't reliable → quote everything

        var e = SQLExecutor.builder(clusterService)
            .addTable("create table doc.summits (x int)")
            .build();
        DocTableInfo table = e.resolveTableInfo("doc.summits");
        Symbol query = e.asSymbol("x > 10 and x < 40");
        List<Reference> columns = List.of(
            table.getReadReference(new ColumnIdent("x"))
        );
        String statement = JdbcBatchIterator.generateStatement(
            table.ident(),
            columns,
            query,
            "\""
        );
        assertThat(statement)
            .isEqualTo("SELECT \"x\" FROM \"doc\".\"summits\" WHERE ((\"x\" > 10) AND (\"x\" < 40))");

        statement = JdbcBatchIterator.generateStatement(
            table.ident(),
            columns,
            query,
            " "
        );
        assertThat(statement)
            .as("JDBC metaData.getIdentifierQuoteString space leads to no quotes at all")
            // See DatabaseMetaData.getIdentifierQuoteString() description:
            //
            //      Retrieves the string used to quote SQL identifiers. This method returns a
            //      space " " if identifier quoting is not supported.
            //
            .isEqualTo("SELECT x FROM doc.summits WHERE ((x > 10) AND (x < 40))");
    }
}

