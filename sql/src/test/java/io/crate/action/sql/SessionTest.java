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

package io.crate.action.sql;

import io.crate.analyze.AnalyzedStatement;
import io.crate.analyze.ParamTypeHints;
import io.crate.analyze.TableDefinitions;
import io.crate.data.Row;
import io.crate.execution.engine.collect.stats.JobsLogs;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.ParameterSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.planner.DependencyCarrier;
import io.crate.sql.parser.SqlParser;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class SessionTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testParameterTypeExtractorNotApplicable() {
        Session.ParameterTypeExtractor typeExtractor = new Session.ParameterTypeExtractor();
        assertThat(typeExtractor.getParameterTypes(s -> {}).length, is(0));
    }

    @Test
    public void testParameterTypeExtractor() {
        Session.ParameterTypeExtractor typeExtractor = new Session.ParameterTypeExtractor();
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
        DataType[] parameterTypes = typeExtractor.getParameterTypes(symbolVisitor);
        assertThat(parameterTypes, equalTo(new DataType[] {
            DataTypes.INTEGER,
            DataTypes.LONG,
            DataTypes.DOUBLE,
            DataTypes.STRING,
        }));

        symbolsToVisit.add(new ParameterSymbol(4, DataTypes.BOOLEAN));
        parameterTypes = typeExtractor.getParameterTypes(symbolVisitor);
        assertThat(parameterTypes, equalTo(new DataType[] {
            DataTypes.INTEGER,
            DataTypes.LONG,
            DataTypes.DOUBLE,
            DataTypes.STRING,
            DataTypes.BOOLEAN
        }));

        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("The assembled list of ParameterSymbols is invalid.");
        // remove the double parameter => make the input invalid
        symbolsToVisit.remove(6);
        typeExtractor.getParameterTypes(symbolVisitor);
    }

    @Test
    public void testGetParamType() {
        SQLExecutor sqlExecutor = SQLExecutor.builder(clusterService).build();

        DependencyCarrier executor = Mockito.mock(DependencyCarrier.class);
        Session session = new Session(
            sqlExecutor.analyzer,
            sqlExecutor.planner,
            new JobsLogs(() -> false),
            false,
            executor,
            SessionContext.create());

        session.parse("S_1", "Select 1 + ? + ?;", Collections.emptyList());
        assertThat(session.getParamType("S_1", 0), is(DataTypes.UNDEFINED));
        assertThat(session.getParamType("S_1", 2), is(DataTypes.UNDEFINED));

        Session.DescribeResult describe = session.describe('S', "S_1");
        assertThat(describe.getParameters(), equalTo(new DataType[] { DataTypes.LONG, DataTypes.LONG }));

        assertThat(session.getParamType("S_1", 0), is(DataTypes.LONG));
        assertThat(session.getParamType("S_1", 1), is(DataTypes.LONG));

        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("Requested parameter index exceeds the number of parameters");
        assertThat(session.getParamType("S_1", 3), is(DataTypes.UNDEFINED));
    }

    @Test
    public void testExtractTypesFromDelete() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService).addDocTable(TableDefinitions.USER_TABLE_INFO).build();
        AnalyzedStatement analyzedStatement = e.analyzer.unboundAnalyze(
            SqlParser.createStatement("delete from users where name = ?"),
            SessionContext.create(),
            ParamTypeHints.EMPTY
        );
        Session.ParameterTypeExtractor typeExtractor = new Session.ParameterTypeExtractor();
        DataType[] parameterTypes = typeExtractor.getParameterTypes(analyzedStatement::visitSymbols);

        assertThat(parameterTypes, is(new DataType[] { DataTypes.STRING }));
    }

    @Test
    public void testExtractTypesFromUpdate() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService).addDocTable(TableDefinitions.USER_TABLE_INFO).build();
        AnalyzedStatement analyzedStatement = e.analyzer.unboundAnalyze(
            SqlParser.createStatement("update users set name = ? || '_updated' where id = ?"),
            SessionContext.create(),
            ParamTypeHints.EMPTY
        );
        Session.ParameterTypeExtractor typeExtractor = new Session.ParameterTypeExtractor();
        DataType[] parameterTypes = typeExtractor.getParameterTypes(analyzedStatement::visitSymbols);

        assertThat(parameterTypes, is(new DataType[] { DataTypes.STRING, DataTypes.LONG }));
    }

    @Test
    public void testExtractTypesFromInsertValues() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService).addDocTable(TableDefinitions.USER_TABLE_INFO).build();
        AnalyzedStatement analyzedStatement = e.analyzer.unboundAnalyze(
            SqlParser.createStatement("INSERT INTO users (id, name) values (?, ?)"),
            SessionContext.create(),
            ParamTypeHints.EMPTY
        );
        Session.ParameterTypeExtractor typeExtractor = new Session.ParameterTypeExtractor();
        DataType[] parameterTypes = typeExtractor.getParameterTypes(analyzedStatement::visitSymbols);

        assertThat(parameterTypes, is(new DataType[] { DataTypes.LONG, DataTypes.STRING }));
    }

    @Test
    public void testExtractTypesFromInsertFromQuery() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService).
            addDocTable(TableDefinitions.USER_TABLE_INFO).
            addDocTable(TableDefinitions.USER_TABLE_INFO_CLUSTERED_BY_ONLY).
            build();
        AnalyzedStatement analyzedStatement = e.analyzer.unboundAnalyze(
            SqlParser.createStatement("INSERT INTO users (id, name) (SELECT id, name FROM users_clustered_by_only " +
                                      "WHERE name = ?)"),
            SessionContext.create(),
            ParamTypeHints.EMPTY
        );
        Session.ParameterTypeExtractor typeExtractor = new Session.ParameterTypeExtractor();
        DataType[] parameterTypes = typeExtractor.getParameterTypes(analyzedStatement::visitSymbols);

        assertThat(parameterTypes, is(new DataType[]{DataTypes.STRING}));
    }

    @Test
    public void testExtractTypesFromInsertWithOnDuplicateKey() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService).
            addDocTable(TableDefinitions.USER_TABLE_INFO).
            addDocTable(TableDefinitions.USER_TABLE_INFO_CLUSTERED_BY_ONLY).
            build();
        AnalyzedStatement analyzedStatement = e.analyzer.unboundAnalyze(
            SqlParser.createStatement("INSERT INTO users (id, name) values (?, ?) " +
                                      "ON DUPLICATE KEY UPDATE name = ?"),
            SessionContext.create(),
            ParamTypeHints.EMPTY
        );
        Session.ParameterTypeExtractor typeExtractor = new Session.ParameterTypeExtractor();
        DataType[] parameterTypes = typeExtractor.getParameterTypes(analyzedStatement::visitSymbols);

        assertThat(parameterTypes, is(new DataType[]{DataTypes.LONG, DataTypes.STRING, DataTypes.STRING}));

        analyzedStatement = e.analyzer.unboundAnalyze(
            SqlParser.createStatement("INSERT INTO users (id, name) (SELECT id, name FROM users_clustered_by_only " +
                                      "WHERE name = ?) ON DUPLICATE KEY UPDATE name = ?"),
            SessionContext.create(),
            ParamTypeHints.EMPTY
        );
        typeExtractor = new Session.ParameterTypeExtractor();
        parameterTypes = typeExtractor.getParameterTypes(analyzedStatement::visitSymbols);

        assertThat(parameterTypes, is(new DataType[]{DataTypes.STRING, DataTypes.STRING}));
    }

    @Test
    public void testProperCleanupOnSessionClose() {
        SQLExecutor sqlExecutor = SQLExecutor.builder(clusterService).build();

        DependencyCarrier executor = Mockito.mock(DependencyCarrier.class);
        Session session = new Session(
            sqlExecutor.analyzer,
            sqlExecutor.planner,
            new JobsLogs(() -> false),
            false,
            executor,
            SessionContext.create());

        session.parse("S_1", "select name from sys.cluster;", Collections.emptyList());
        session.bind("Portal", "S_1", Collections.emptyList(), null);
        session.describe('S', "S_1");

        session.parse("S_2", "select id from sys.cluster", Collections.emptyList());
        session.bind("", "S_2", Collections.emptyList(), null);
        session.describe('S', "S_2");
        session.execute("", 0, new BaseResultReceiver());

        assertThat(session.portals.size(), is(2));
        assertThat(session.preparedStatements.size(), is(2));
        assertThat(session.pendingExecutions.size(), is(1));

        session.close();

        assertThat(session.portals.size(), is(0));
        assertThat(session.preparedStatements.size(), is(0));
        assertThat(session.pendingExecutions.size(), is(0));
    }

    @Test
    public void testDeallocateAllClearsAllPortalsAndPreparedStatements() {
        SQLExecutor sqlExecutor = SQLExecutor.builder(clusterService).build();

        DependencyCarrier executor = Mockito.mock(DependencyCarrier.class);
        Session session = new Session(
            sqlExecutor.analyzer,
            sqlExecutor.planner,
            new JobsLogs(() -> false),
            false,
            executor,
            SessionContext.create());

        session.parse("S_1", "select * from sys.cluster;", Collections.emptyList());
        session.bind("Portal", "S_1", Collections.emptyList(), null);
        session.describe('S', "S_1");

        session.parse("S_2", "DEALLOCATE ALL;", Collections.emptyList());
        session.bind("", "S_2", Collections.emptyList(), null);
        session.execute("", 0, new BaseResultReceiver() {
            @Override
            public void setNextRow(Row row) {
            }
        });

        assertThat(session.portals.size(), greaterThan(0));
        assertThat(session.preparedStatements.size(), is(0));
    }

    @Test
    public void testDeallocatePreparedStatementClearsPreparedStatement() {
        SQLExecutor sqlExecutor = SQLExecutor.builder(clusterService).build();

        DependencyCarrier executor = Mockito.mock(DependencyCarrier.class);
        Session session = new Session(
            sqlExecutor.analyzer,
            sqlExecutor.planner,
            new JobsLogs(() -> false),
            false,
            executor,
            SessionContext.create());

        session.parse("test_prep_stmt", "select * from sys.cluster;", Collections.emptyList());
        session.bind("Portal", "test_prep_stmt", Collections.emptyList(), null);
        session.describe('S', "test_prep_stmt");

        session.parse("stmt", "DEALLOCATE test_prep_stmt;", Collections.emptyList());
        session.bind("", "stmt", Collections.emptyList(), null);
        session.execute("", 0, new BaseResultReceiver() {
            @Override
            public void setNextRow(Row row) {
            }
        });

        assertThat(session.portals.size(), greaterThan(0));
        assertThat(session.preparedStatements.size(), is(1));
        assertThat(session.preparedStatements.get("stmt").query(), is("DEALLOCATE test_prep_stmt;"));
    }
}
