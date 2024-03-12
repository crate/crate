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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.junit.Test;

import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.ParameterSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.sql.parser.SqlParser;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public class ParameterTypesTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testParameterTypeExtractorNotApplicable() {
        assertThat(ParameterTypes.extract(s -> {})).isEmpty();
    }

    @Test
    public void testParameterTypeExtractor() {
        List<Symbol> symbolsToVisit = new ArrayList<>();
        symbolsToVisit.add(Literal.of(1));
        symbolsToVisit.add(Literal.of("foo"));
        symbolsToVisit.add(new ParameterSymbol(1, DataTypes.LONG));
        symbolsToVisit.add(new ParameterSymbol(0, DataTypes.INTEGER));
        symbolsToVisit.add(new ParameterSymbol(3, DataTypes.STRING));
        symbolsToVisit.add(Literal.of("bar"));
        symbolsToVisit.add(new ParameterSymbol(2, DataTypes.DOUBLE));
        symbolsToVisit.add(new ParameterSymbol(1, DataTypes.LONG));
        symbolsToVisit.add(new ParameterSymbol(0, DataTypes.INTEGER));
        symbolsToVisit.add(Literal.of(1.2));

        Consumer<Consumer<? super Symbol>> symbolVisitor = c -> {
            for (Symbol symbol : symbolsToVisit) {
                c.accept(symbol);
            }
        };
        List<DataType<?>> parameterTypes = ParameterTypes.extract(symbolVisitor);
        assertThat(parameterTypes).containsExactly(
            DataTypes.INTEGER,
            DataTypes.LONG,
            DataTypes.DOUBLE,
            DataTypes.STRING
        );

        symbolsToVisit.add(new ParameterSymbol(4, DataTypes.BOOLEAN));
        parameterTypes = ParameterTypes.extract(symbolVisitor);
        assertThat(parameterTypes).containsExactly(
            DataTypes.INTEGER,
            DataTypes.LONG,
            DataTypes.DOUBLE,
            DataTypes.STRING,
            DataTypes.BOOLEAN
        );

        // remove the double parameter => make the input invalid
        symbolsToVisit.remove(6);
        assertThatThrownBy(() -> ParameterTypes.extract(symbolVisitor))
            .isExactlyInstanceOf(IllegalStateException.class)
            .hasMessage("The assembled list of ParameterSymbols is invalid. Missing parameters.");
    }

    @Test
    public void testExtractTypesFromDelete() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable(TableDefinitions.USER_TABLE_DEFINITION);
        AnalyzedStatement analyzedStatement = e.analyzer.analyze(
            SqlParser.createStatement("delete from users where name = ?"),
            CoordinatorSessionSettings.systemDefaults(),
            ParamTypeHints.EMPTY,
            e.cursors);
        List<DataType<?>> parameterTypes = ParameterTypes.extract(analyzedStatement);
        assertThat(parameterTypes).containsExactly(DataTypes.STRING);
    }

    @Test
    public void testExtractTypesFromUpdate() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable(TableDefinitions.USER_TABLE_DEFINITION);
        AnalyzedStatement analyzedStatement = e.analyzer.analyze(
            SqlParser.createStatement("update users set name = ? || '_updated' where id = ?"),
            CoordinatorSessionSettings.systemDefaults(),
            ParamTypeHints.EMPTY,
            e.cursors);
        List<DataType<?>> parameterTypes = ParameterTypes.extract(analyzedStatement);
        assertThat(parameterTypes).containsExactly(DataTypes.STRING, DataTypes.LONG);
    }

    @Test
    public void testExtractTypesFromInsertValues() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable(TableDefinitions.USER_TABLE_DEFINITION);
        AnalyzedStatement analyzedStatement = e.analyzer.analyze(
            SqlParser.createStatement("INSERT INTO users (id, name) values (?, ?)"),
            CoordinatorSessionSettings.systemDefaults(),
            ParamTypeHints.EMPTY,
            e.cursors
        );
        List<DataType<?>> parameterTypes = ParameterTypes.extract(analyzedStatement);
        assertThat(parameterTypes).containsExactly(DataTypes.LONG, DataTypes.STRING);
    }

    @Test
    public void testExtractTypesFromInsertFromQuery() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .addTable(TableDefinitions.USER_TABLE_CLUSTERED_BY_ONLY_DEFINITION);
        AnalyzedStatement analyzedStatement = e.analyzer.analyze(
            SqlParser.createStatement("INSERT INTO users (id, name) (SELECT id, name FROM users_clustered_by_only " +
                                      "WHERE name = ?)"),
            CoordinatorSessionSettings.systemDefaults(),
            ParamTypeHints.EMPTY,
            e.cursors
        );
        List<DataType<?>> parameterTypes = ParameterTypes.extract(analyzedStatement);
        assertThat(parameterTypes).containsExactly(DataTypes.STRING);
    }

    @Test
    public void testExtractTypesFromInsertWithOnDuplicateKey() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .addTable(TableDefinitions.USER_TABLE_CLUSTERED_BY_ONLY_DEFINITION);
        AnalyzedStatement analyzedStatement = e.analyzer.analyze(
            SqlParser.createStatement("INSERT INTO users (id, name) values (?, ?) " +
                                      "ON CONFLICT (id) DO UPDATE SET name = ?"),
            CoordinatorSessionSettings.systemDefaults(),
            ParamTypeHints.EMPTY,
            e.cursors);
        List<DataType<?>> parameterTypes = ParameterTypes.extract(analyzedStatement);

        assertThat(parameterTypes).containsExactly(DataTypes.LONG, DataTypes.STRING, DataTypes.STRING);

        analyzedStatement = e.analyzer.analyze(
            SqlParser.createStatement("INSERT INTO users (id, name) (SELECT id, name FROM users_clustered_by_only " +
                                      "WHERE name = ?) ON CONFLICT (id) DO UPDATE SET name = ?"),
            CoordinatorSessionSettings.systemDefaults(),
            ParamTypeHints.EMPTY,
            e.cursors);
        parameterTypes = ParameterTypes.extract(analyzedStatement);

        assertThat(parameterTypes).containsExactly(DataTypes.STRING, DataTypes.STRING);
    }

    @Test
    public void testTypesCanBeResolvedIfParametersAreInSubRelation() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService);

        AnalyzedStatement stmt = e.analyzer.analyze(
            SqlParser.createStatement("select * from (select $1::int + $2) t"),
            CoordinatorSessionSettings.systemDefaults(),
            ParamTypeHints.EMPTY,
            e.cursors
        );
        List<DataType<?>> parameterTypes = ParameterTypes.extract(stmt);
        assertThat(parameterTypes).containsExactly(DataTypes.INTEGER, DataTypes.INTEGER);
    }

    @Test
    public void testTypesCanBeResolvedIfParametersAreInSubRelationOfInsertStatement() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table t (x int)");

        AnalyzedStatement stmt = e.analyzer.analyze(
            SqlParser.createStatement("insert into t (x) (select * from (select $1::int + $2) t)"),
            CoordinatorSessionSettings.systemDefaults(),
            ParamTypeHints.EMPTY,
            e.cursors
        );
        List<DataType<?>> parameterTypes = ParameterTypes.extract(stmt);
        assertThat(parameterTypes).containsExactly(DataTypes.INTEGER, DataTypes.INTEGER);
    }

    @Test
    public void testTypesCanBeResolvedIfParametersAreInSubQueryInDeleteStatement() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table t (x int)");

        AnalyzedStatement stmt = e.analyzer.analyze(
            SqlParser.createStatement("delete from t where x = (select $1::long)"),
            CoordinatorSessionSettings.systemDefaults(),
            ParamTypeHints.EMPTY,
            e.cursors
        );
        List<DataType<?>> parameterTypes = ParameterTypes.extract(stmt);
        assertThat(parameterTypes).containsExactly(DataTypes.LONG);
    }


    @Test
    public void test_can_extract_parameters_from_match_predicate() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table users (name text, keywords text)");
        AnalyzedStatement statement = e.analyze(
            "select * from users where match(keywords, ?) using best_fields with (fuzziness= ?) and name = ?");
        List<DataType<?>> parameterTypes = ParameterTypes.extract(statement);
        assertThat(parameterTypes).containsExactly(DataTypes.STRING, DataTypes.UNDEFINED, DataTypes.STRING);
    }


    @Test
    public void test_can_extract_parameters_from_join_condition() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table subscriptions (id text primary key, name text not null)")
            .addTable("create table clusters (id text, subscription_id text)");

        AnalyzedStatement stmt = e.analyze(
            """
                    select
                        *
                    from subscriptions
                    join clusters on clusters.subscription_id = subscriptions.id
                        AND subscriptions.name = ?
                """);
        List<DataType<?>> parameterTypes = ParameterTypes.extract(stmt);
        assertThat(parameterTypes).containsExactly(DataTypes.STRING);
    }


    @Test
    public void test_can_extract_parameters_from_create_table_defaults() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService);
        AnalyzedStatement stmt = e.analyze("create table tbl (x int, y int default $1)");
        List<DataType<?>> parameterTypes = ParameterTypes.extract(stmt);
        assertThat(parameterTypes).containsExactly(DataTypes.INTEGER);
    }
}
