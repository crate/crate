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

package io.crate.analyze;

import com.google.common.collect.ImmutableList;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.TableFunctionRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.common.collections.Lists2;
import io.crate.exceptions.AmbiguousColumnException;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.exceptions.ConversionException;
import io.crate.exceptions.RelationUnknown;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.execution.engine.aggregation.impl.AverageAggregation;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.operator.LikeOperators;
import io.crate.expression.operator.LteOperator;
import io.crate.expression.operator.OrOperator;
import io.crate.expression.operator.RegexpMatchOperator;
import io.crate.expression.operator.any.AnyOperators;
import io.crate.expression.predicate.IsNullPredicate;
import io.crate.expression.predicate.NotPredicate;
import io.crate.expression.scalar.SubscriptFunction;
import io.crate.expression.scalar.arithmetic.ArithmeticFunctions;
import io.crate.expression.scalar.geo.DistanceFunction;
import io.crate.expression.symbol.AliasSymbol;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.MatchPredicate;
import io.crate.expression.symbol.ParameterSymbol;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolType;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.RelationName;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.sql.parser.ParsingException;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.hamcrest.Matchers;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static io.crate.testing.RelationMatchers.isDocTable;
import static io.crate.testing.SymbolMatchers.isAlias;
import static io.crate.testing.SymbolMatchers.isField;
import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isLiteral;
import static io.crate.testing.SymbolMatchers.isReference;
import static io.crate.testing.TestingHelpers.isSQL;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.core.Is.is;

@SuppressWarnings("ConstantConditions")
public class SelectStatementAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor sqlExecutor;

    @Before
    public void prepare() throws IOException {
        sqlExecutor = SQLExecutor.builder(clusterService)
            .enableDefaultTables()
            .addTable("create table foo.users (id bigint primary key, name text)")
            .addTable("create table fooobar (id bigint primary key, name text)")
            .addTable("create table \"Foobaarr\" (id bigint primary key, name text)")
            .build();
    }

    private <T extends AnalyzedRelation> T analyze(String statement) {
        return sqlExecutor.analyze(statement);
    }

    @Test
    public void testIsNullQuery() {
        QueriedSelectRelation relation = analyze("select * from sys.nodes where id is not null");
        assertThat(relation.where().hasQuery(), is(true));
        Function query = (Function) relation.where().query();

        assertThat(query.info().ident().name(), is(NotPredicate.NAME));
        assertThat(query.arguments().get(0), instanceOf(Function.class));
        Function isNull = (Function) query.arguments().get(0);
        assertThat(isNull.info().ident().name(), is(IsNullPredicate.NAME));
    }

    @Test
    public void testQueryUsesSearchPath() throws IOException {
        SQLExecutor executor = SQLExecutor.builder(clusterService)
            .setSearchPath("first", "second", "third")
            .addTable("create table \"first\".t (id int)")
            .addTable("create table third.t1 (id int)")
            .build();

        QueriedSelectRelation queriedTable = executor.analyze("select * from t");
        assertThat(queriedTable.from(), contains(isDocTable(new RelationName("first", "t"))));

        queriedTable = executor.analyze("select * from t1");
        assertThat(queriedTable.from(), contains(isDocTable(new RelationName("third", "t1"))));
    }

    @Test
    public void testOrderedSelect() throws Exception {
        QueriedSelectRelation table = analyze("select load['1'] from sys.nodes order by load['5'] desc");
        assertThat(table.limit(), nullValue());

        assertThat(table.groupBy().isEmpty(), is(true));
        assertThat(table.orderBy(), notNullValue());

        assertThat(table.outputs().size(), is(1));
        assertThat(table.orderBy().orderBySymbols().size(), is(1));
        assertThat(table.orderBy().reverseFlags().length, is(1));

        assertThat(table.orderBy().orderBySymbols().get(0), isReference("load['5']"));
    }

    @Test
    public void testNegativeLiteral() throws Exception {
        QueriedSelectRelation relation =  analyze("select * from sys.nodes where port['http'] = -400");
        Function whereClause = (Function) relation.where().query();
        Symbol symbol = whereClause.arguments().get(1);
        assertThat(((Literal) symbol).value(), is(-400));
    }

    @Test
    public void testSimpleSelect() throws Exception {
        QueriedSelectRelation relation = analyze("select load['5'] from sys.nodes limit 2");
        assertThat(relation.limit(), is(Literal.of(2L)));

        assertThat(relation.groupBy().isEmpty(), is(true));
        assertThat(relation.outputs().size(), is(1));
        assertThat(relation.outputs().get(0), isReference("load['5']"));
    }

    @Test
    public void testAggregationSelect() throws Exception {
        QueriedSelectRelation relation = analyze("select avg(load['5']) from sys.nodes");
        assertThat(relation.groupBy().isEmpty(), is(true));
        assertThat(relation.outputs().size(), is(1));
        Function col1 = (Function) relation.outputs().get(0);
        assertThat(col1.info().type(), is(FunctionInfo.Type.AGGREGATE));
        assertThat(col1.info().ident().name(), is(AverageAggregation.NAME));
    }

    private List<String> outputNames(AnalyzedRelation relation) {
        return Lists2.map(relation.outputs(), x -> Symbols.pathFromSymbol(x).sqlFqn());
    }

    @Test
    public void testAllColumnCluster() throws Exception {
        AnalyzedRelation relation = analyze("select * from sys.cluster");
        assertThat(relation.outputs().size(), is(5));
        assertThat(outputNames(relation), containsInAnyOrder("id", "license", "master_node", "name", "settings"));
        assertThat(relation.outputs().size(), is(5));
    }

    @Test
    public void testAllColumnNodes() throws Exception {
        AnalyzedRelation relation = analyze("select id, * from sys.nodes");
        List<String> outputNames = outputNames(relation);
        assertThat(outputNames, contains(
            "id",
            "cluster_state_version",
            "connections",
            "fs",
            "heap",
            "hostname",
            "id",
            "load",
            "mem",
            "name",
            "network",
            "os",
            "os_info",
            "port",
            "process",
            "rest_url",
            "thread_pools",
            "version"
        ));
        assertThat(relation.outputs().size(), is(outputNames.size()));
    }

    @Test
    public void testWhereSelect() throws Exception {
        QueriedSelectRelation relation = analyze("select load from sys.nodes " +
                                            "where load['1'] = 1.2 or 1 >= load['5']");

        assertThat(relation.groupBy().isEmpty(), is(true));

        Function whereClause = (Function) relation.where().query();
        assertThat(whereClause.info().ident().name(), is(OrOperator.NAME));
        assertThat(whereClause.info().type() == FunctionInfo.Type.AGGREGATE, is(false));

        Function left = (Function) whereClause.arguments().get(0);
        assertThat(left.info().ident().name(), is(EqOperator.NAME));

        assertThat(left.arguments().get(0), isReference("load['1']"));

        assertThat(left.arguments().get(1), IsInstanceOf.instanceOf(Literal.class));
        assertThat(left.arguments().get(1).valueType(), is(DataTypes.DOUBLE));

        Function right = (Function) whereClause.arguments().get(1);
        assertThat(right.info().ident().name(), is(LteOperator.NAME));
        assertThat(right.arguments().get(0), isReference("load['5']"));
        assertThat(right.arguments().get(1), IsInstanceOf.instanceOf(Literal.class));
        assertThat(left.arguments().get(1).valueType(), is(DataTypes.DOUBLE));
    }

    @Test
    public void testSelectWithParameters() throws Exception {
        QueriedSelectRelation relation = analyze(
            "select load from sys.nodes " +
            "where load['1'] = ? or load['5'] <= ? or load['15'] >= ? or load['1'] = ? " +
            "or load['1'] = ? or name = ?");
        Function whereClause = (Function) relation.where().query();
        assertThat(whereClause.info().ident().name(), is(OrOperator.NAME));
        assertThat(whereClause.info().type() == FunctionInfo.Type.AGGREGATE, is(false));

        Function function = (Function) whereClause.arguments().get(0);
        assertThat(function.info().ident().name(), is(OrOperator.NAME));
        function = (Function) function.arguments().get(1);
        assertThat(function.info().ident().name(), is(EqOperator.NAME));
        assertThat(function.arguments().get(1), IsInstanceOf.instanceOf(ParameterSymbol.class));
        assertThat(function.arguments().get(1).valueType(), is(DataTypes.DOUBLE));

        function = (Function) whereClause.arguments().get(1);
        assertThat(function.info().ident().name(), is(EqOperator.NAME));
        assertThat(function.arguments().get(1), IsInstanceOf.instanceOf(ParameterSymbol.class));
        assertThat(function.arguments().get(1).valueType(), is(DataTypes.STRING));
    }

    @Test
    public void testOutputNames() throws Exception {
        AnalyzedRelation relation = analyze("select load as l, id, load['1'] from sys.nodes");
        List<String> outputNames = outputNames(relation);
        assertThat(outputNames.size(), is(3));
        assertThat(outputNames.get(0), is("l"));
        assertThat(outputNames.get(1), is("id"));
        assertThat(outputNames.get(2), is("load['1']"));
    }

    @Test
    public void testDuplicateOutputNames() throws Exception {
        AnalyzedRelation relation = analyze("select load as l, load['1'] as l from sys.nodes");
        List<String> outputNames = outputNames(relation);
        assertThat(outputNames.size(), is(2));
        assertThat(outputNames.get(0), is("l"));
        assertThat(outputNames.get(1), is("l"));
    }

    @Test
    public void testOrderByOnAlias() throws Exception {
        QueriedSelectRelation relation = analyze(
            "select name as cluster_name from sys.cluster order by cluster_name");
        List<String> outputNames = outputNames(relation);
        assertThat(outputNames.size(), is(1));
        assertThat(outputNames.get(0), is("cluster_name"));

        assertThat(relation.orderBy(), notNullValue());
        assertThat(relation.orderBy().orderBySymbols().size(), is(1));
        assertThat(relation.orderBy().orderBySymbols().get(0), is(relation.outputs().get(0)));
    }

    @Test
    public void testSelectGlobalAggregationOrderByWithColumnMissingFromSelect() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("ORDER BY expression 'id' must appear in the select clause " +
                                        "when grouping or global aggregation is used");
        analyze("select count(id) from users order by id");
    }

    @Test
    public void testValidCombinationsOrderByWithAggregation() throws Exception {
        analyze("select name, count(id) from users group by name order by 1");
        analyze("select name, count(id) from users group by name order by 2");

        analyze("select name, count(id) from users group by name order by name");
        analyze("select name, count(id) from users group by name order by count(id)");

        analyze("select name, count(id) from users group by name order by lower(name)");
        analyze("select name, count(id) from users group by name order by lower(upper(name))");

        analyze("select name, count(id) from users group by name order by sin(count(id))");
        analyze("select name, count(id) from users group by name order by sin(sqrt(count(id)))");
    }

    @Test
    public void testOffsetSupportInAnalyzer() throws Exception {
        QueriedSelectRelation relation = analyze("select * from sys.nodes limit 1 offset 3");
        assertThat(relation.offset(), is(Literal.of(3L)));
    }

    @Test
    public void testNoMatchStatement() throws Exception {
        for (String stmt : ImmutableList.of(
            "select id from sys.nodes where false",
            "select id from sys.nodes where 1=0"
        )) {
            QueriedSelectRelation relation = analyze(stmt);
            assertThat(stmt, relation.where().queryOrFallback(), isLiteral(false));
        }
    }

    @Test
    public void testEvaluatingMatchAllStatement() throws Exception {
        QueriedSelectRelation relation = analyze("select id from sys.nodes where 1 = 1");
        assertThat(relation.where().queryOrFallback(), isLiteral(true));
    }

    @Test
    public void testAllMatchStatement() throws Exception {
        for (String stmt : ImmutableList.of(
            "select id from sys.nodes where true",
            "select id from sys.nodes where 1=1",
            "select id from sys.nodes"
        )) {
            QueriedSelectRelation relation = analyze(stmt);
            assertThat(stmt, relation.where().queryOrFallback(), isLiteral(true));
        }
    }

    @Test
    public void testRewriteNotEquals() {
        // should rewrite to:
        //    not(eq(sys.noes.name, 'something'))
        ImmutableList<String> statements = ImmutableList.of(
            "select * from sys.nodes where sys.nodes.name <> 'something'",
            "select * from sys.nodes where sys.nodes.name != 'something'"
        );
        for (String statement : statements) {
            QueriedSelectRelation relation = analyze(statement);
            WhereClause whereClause = relation.where();

            Function notFunction = (Function) whereClause.query();
            assertThat(notFunction.info().ident().name(), is(NotPredicate.NAME));
            assertThat(notFunction.arguments().size(), is(1));

            Function eqFunction = (Function) notFunction.arguments().get(0);
            assertThat(eqFunction.info().ident().name(), is(EqOperator.NAME));
            assertThat(eqFunction.arguments().size(), is(2));

            List<Symbol> eqArguments = eqFunction.arguments();
            assertThat(eqArguments.get(1), isLiteral("something"));
        }
    }

    @Test
    public void testRewriteRegexpNoMatch() throws Exception {
        String statement = "select * from sys.nodes where sys.nodes.name !~ '[sS]omething'";
        QueriedSelectRelation relation = analyze(statement);
        WhereClause whereClause = relation.where();

        Function notFunction = (Function) whereClause.query();
        assertThat(notFunction.info().ident().name(), is(NotPredicate.NAME));
        assertThat(notFunction.arguments().size(), is(1));

        Function eqFunction = (Function) notFunction.arguments().get(0);
        assertThat(eqFunction.info().ident().name(), is(RegexpMatchOperator.NAME));
        assertThat(eqFunction.arguments().size(), is(2));

        List<Symbol> eqArguments = eqFunction.arguments();

        assertThat(eqArguments.get(0), isReference("name"));
        assertThat(eqArguments.get(1), isLiteral("[sS]omething"));
    }

    @Test
    public void testGranularityWithSingleAggregation() throws Exception {
        QueriedSelectRelation table = analyze("select count(*) from sys.nodes");
        assertEquals(((TableRelation) table.from().get(0)).tableInfo().ident(), SysNodesTableInfo.IDENT);
    }

    @Test
    public void testRewriteCountStringLiteral() {
        AnalyzedRelation relation = analyze("select count('id') from sys.nodes");
        List<Symbol> outputSymbols = relation.outputs();
        assertThat(outputSymbols.size(), is(1));
        assertThat(outputSymbols.get(0), instanceOf(Function.class));
        assertThat(((Function) outputSymbols.get(0)).arguments().size(), is(0));
    }

    @Test
    public void testRewriteCountNull() {
        AnalyzedRelation relation = analyze("select count(null) from sys.nodes");
        List<Symbol> outputSymbols = relation.outputs();
        assertThat(outputSymbols.size(), is(1));
        assertThat(outputSymbols.get(0), instanceOf(Literal.class));
        assertThat(((Literal) outputSymbols.get(0)).value(), is(0L));
    }

    @Test
    public void testWhereInSelect() throws Exception {
        QueriedSelectRelation relation = analyze("select load from sys.nodes where load['1'] in (1.0, 2.0, 4.0, 8.0, 16.0)");
        Function whereClause = (Function) relation.where().query();
        assertThat(whereClause.info().ident().name(), is(AnyOperators.Names.EQ));
    }

    @Test
    public void testWhereInSelectListWithNull() throws Exception {
        QueriedSelectRelation relation = analyze("select 'found' from users where 1 in (3, 2, null)");
        assertThat(relation.where().queryOrFallback(), isLiteral(null));
    }

    @Test
    public void testWhereInSelectValueIsNull() throws Exception {
        QueriedSelectRelation relation = analyze("select 'found' from users where null in (1, 2)");
        assertThat(relation.where().queryOrFallback(), isLiteral(null));
    }

    @Test
    public void testWhereInSelectDifferentDataTypeValue() throws Exception {
        QueriedSelectRelation relation;
        relation = analyze("select 'found' from users where 1.2 in (1, 2)");
        assertThat(relation.where().queryOrFallback(), isLiteral(false)); // already normalized to 1.2 in (1.0, 2.0) --> false
        relation = analyze("select 'found' from users where 1 in (1.2, 2)");
        assertThat(relation.where().queryOrFallback(), isLiteral(false));
    }

    @Test
    public void testWhereInSelectDifferentDataTypeValueIncompatibleDataTypes() throws Exception {
        expectedException.expect(ConversionException.class);
        expectedException.expectMessage("Cannot cast `'foo'` of type `text` to type `bigint`");
        analyze("select 'found' from users where 1 in (1, 'foo', 2)");
    }

    @Test
    public void testAggregationDistinct() {
        AnalyzedRelation relation = analyze("select count(distinct load['1']) from sys.nodes");

        Symbol output = relation.outputs().get(0);
        assertThat(output, isFunction("collection_count"));

        Function collectionCount = (Function) output;
        assertThat(collectionCount.arguments().size(), is(1));
        Symbol symbol = collectionCount.arguments().get(0);
        assertThat(symbol, isFunction("collect_set"));

        Function collectSet = (Function) symbol;
        assertThat(collectSet.info().type(), equalTo(FunctionInfo.Type.AGGREGATE));

        assertThat(collectSet.arguments().size(), is(1));
        assertThat(collectSet.arguments().get(0), isReference("load['1']"));
    }

    @Test
    public void testSelectDistinctWithFunction() {
        QueriedSelectRelation relation = analyze("select distinct id + 1 from users");
        assertThat(relation.isDistinct(), is(true));
        assertThat(relation.outputs(), isSQL("(doc.users.id + 1)"));
    }

    @Test
    public void testSelectDistinctWithGroupBySameFieldsSameOrder() {
        QueriedSelectRelation distinctRelation = analyze("select distinct id, name from users group by id, name");
        QueriedSelectRelation groupByRelation = analyze("select id, name from users group by id, name");
        assertThat(distinctRelation.groupBy(), equalTo(groupByRelation.groupBy()));
        assertThat(distinctRelation.outputs(), equalTo(groupByRelation.outputs()));
    }

    @Test
    public void testSelectDistinctWithGroupBySameFieldsDifferentOrder() {
        AnalyzedRelation relation = analyze("select distinct name, id from users group by id, name");
        assertThat(
            relation,
            isSQL("SELECT doc.users.name, doc.users.id GROUP BY doc.users.id, doc.users.name"));
    }

    @Test
    public void testDistinctOnLiteral() {
        QueriedSelectRelation relation = analyze("select distinct [1,2,3] from users");
        assertThat(relation.isDistinct(), is(true));
        assertThat(relation.outputs(), isSQL("[1, 2, 3]"));
    }

    @Test
    public void testDistinctOnNullLiteral() {
        QueriedSelectRelation relation = analyze("select distinct null from users");
        assertThat(relation.isDistinct(), is(true));
        assertThat(relation.outputs(), isSQL("NULL"));
    }

    @Test
    public void testSelectGlobalDistinctAggregate() {
        QueriedSelectRelation relation = analyze("select distinct count(*) from users");
        assertThat(relation.groupBy().isEmpty(), is(true));
    }

    @Test
    public void testSelectGlobalDistinctRewriteAggregationGroupBy() {
        QueriedSelectRelation distinctRelation = analyze("select distinct name, count(id) from users group by name");
        QueriedSelectRelation groupByRelation = analyze("select name, count(id) from users group by name");
        assertEquals(groupByRelation.groupBy(), distinctRelation.groupBy());
    }

    @Test
    public void testSelectWithObjectLiteral() throws Exception {
        QueriedSelectRelation relation = analyze("select id from sys.nodes where load={\"1\"=1.0}");
        Function whereClause = (Function) relation.where().query();
        assertThat(whereClause.arguments(), hasItem(isLiteral(Map.of("1", 1.0))));
    }

    @Test
    public void testLikeInWhereQuery() {
        QueriedSelectRelation relation = analyze("select * from sys.nodes where name like 'foo'");

        assertNotNull(relation.where());
        Function whereClause = (Function) relation.where().query();
        assertThat(whereClause.info().ident().name(), is(LikeOperators.OP_LIKE));
        ImmutableList<DataType> argumentTypes = ImmutableList.of(DataTypes.STRING, DataTypes.STRING);
        assertEquals(argumentTypes, whereClause.info().ident().argumentTypes());

        assertThat(whereClause.arguments().get(0), isReference("name"));
        assertThat(whereClause.arguments().get(1), isLiteral("foo"));
    }

    @Test
    public void testILikeInWhereQuery() {
        QueriedSelectRelation relation = analyze("select * from sys.nodes where name ilike 'foo%'");

        assertNotNull(relation.where());
        Function whereClause = (Function) relation.where().query();
        assertThat(whereClause.info().ident().name(), is(LikeOperators.OP_ILIKE));
        ImmutableList<DataType> argumentTypes = ImmutableList.of(DataTypes.STRING, DataTypes.STRING);
        assertEquals(argumentTypes, whereClause.info().ident().argumentTypes());

        assertThat(whereClause.arguments().get(0), isReference("name"));
        assertThat(whereClause.arguments().get(1), isLiteral("foo%"));
    }

    @Test
    public void testLikeEscapeInWhereQuery() {
        // ESCAPE is not supported yet
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("ESCAPE is not supported.");
        analyze("select * from sys.nodes where name like 'foo' escape 'o'");
    }

    @Test
    public void testILikeEscapeInWhereQuery() {
        // ESCAPE is not supported yet
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("ESCAPE is not supported.");
        analyze("select * from sys.nodes where name ilike 'foo%' escape 'o'");
    }

    @Test
    public void testLikeNoStringDataTypeInWhereQuery() {
        QueriedSelectRelation relation = analyze("select * from sys.nodes where name like 1");

        // check if the implicit cast of the pattern worked
        ImmutableList<DataType> argumentTypes = ImmutableList.of(DataTypes.STRING, DataTypes.STRING);
        Function whereClause = (Function) relation.where().query();
        assertEquals(argumentTypes, whereClause.info().ident().argumentTypes());
        assertThat(whereClause.arguments().get(1), IsInstanceOf.instanceOf(Literal.class));
        Literal stringLiteral = (Literal) whereClause.arguments().get(1);
        assertThat(stringLiteral.value(), is("1"));
    }

    @Test
    public void testLikeLongDataTypeInWhereQuery() {
        QueriedSelectRelation relation = analyze("select * from sys.nodes where 1 like 2");
        assertThat(relation.where().queryOrFallback(), isLiteral(false));
    }

    @Test
    public void testILikeLongDataTypeInWhereQuery() {
        QueriedSelectRelation relation = analyze("select * from sys.nodes where 1 ilike 2");
        assertThat(relation.where().queryOrFallback(), isLiteral(false));
    }

    @Test
    public void testIsNullInWhereQuery() {
        QueriedSelectRelation relation = analyze("select * from sys.nodes where name is null");
        Function isNullFunction = (Function) relation.where().query();

        assertThat(isNullFunction.info().ident().name(), is(IsNullPredicate.NAME));
        assertThat(isNullFunction.arguments().size(), is(1));
        assertThat(isNullFunction.arguments().get(0), isReference("name"));
        assertNotNull(relation.where());
    }

    @Test
    public void testNullIsNullInWhereQuery() {
        QueriedSelectRelation relation = analyze("select * from sys.nodes where null is null");
        assertThat(relation.where(), is(WhereClause.MATCH_ALL));
    }

    @Test
    public void testLongIsNullInWhereQuery() {
        QueriedSelectRelation relation = analyze("select * from sys.nodes where 1 is null");
        assertThat(relation.where().queryOrFallback(), isSQL("false"));
    }

    @Test
    public void testNotPredicate() {
        QueriedSelectRelation relation = analyze("select * from users where name not like 'foo%'");
        assertThat(((Function) relation.where().query()).info().ident().name(), is(NotPredicate.NAME));
    }

    @Test
    public void testFilterByLiteralBoolean() throws Exception {
        QueriedSelectRelation relation = analyze("select * from users where awesome=TRUE");
        assertThat(((Function) relation.where().query()).arguments().get(1).symbolType(),
            is(SymbolType.LITERAL));
    }

    @Test
    public void testSelectColumnWitoutFromResultsInColumnUnknownException() throws Exception {
        expectedException.expect(ColumnUnknownException.class);
        expectedException.expectMessage("Column name unknown");
        analyze("select 'bar', name");
    }

    @Test
    public void test2From() throws Exception {
        AnalyzedRelation relation = analyze("select a.name from users a, users b");
        assertThat(relation, instanceOf(QueriedSelectRelation.class));
    }

    @Test
    public void testOrderByQualifiedName() throws Exception {
        expectedException.expect(RelationUnknown.class);
        expectedException.expectMessage("Relation 'doc.friends' unknown");
        analyze("select * from users order by friends.id");
    }

    @Test
    public void testNotTimestamp() throws Exception {
        expectedException.expect(ConversionException.class);
        expectedException.expectMessage("Cannot cast `date` of type `timestamp with time zone` to type `boolean`");
        analyze("select id, name from parted where not date");
    }

    @Test
    public void testJoin() throws Exception {
        AnalyzedRelation relation = analyze("select * from users, users_multi_pk where users.id = users_multi_pk.id");
        assertThat(relation, instanceOf(QueriedSelectRelation.class));
    }

    @Test
    public void testInnerJoinSyntaxDoesNotExtendsWhereClause() throws Exception {
        QueriedSelectRelation mss = (QueriedSelectRelation) analyze(
            "select * from users inner join users_multi_pk on users.id = users_multi_pk.id");
        assertThat(mss.where().query(), isSQL("null"));
        assertThat(mss.joinPairs().get(0).condition(),
            isSQL("(doc.users.id = doc.users_multi_pk.id)"));
    }

    @Test
    public void testJoinSyntaxWithMoreThan2Tables() throws Exception {
        QueriedSelectRelation relation = analyze("select * from users u1 " +
                                                 "join users_multi_pk u2 on u1.id = u2.id " +
                                                 "join users_clustered_by_only u3 on u2.id = u3.id ");
        assertThat(relation.where().query(), isSQL("null"));

        assertThat(relation.joinPairs().get(0).condition(),
            isSQL("(u1.id = u2.id)"));
        assertThat(relation.joinPairs().get(1).condition(),
            isSQL("(u2.id = u3.id)"));
    }

    @Test
    public void testCrossJoinWithJoinCondition() throws Exception {
        expectedException.expect(ParsingException.class);
        analyze("select * from users cross join users_multi_pk on users.id = users_multi_pk.id");
    }

    @Test
    public void testJoinUsingSyntax() throws Exception {
        QueriedSelectRelation relation = analyze("select * from users join users_multi_pk using (id, name)");
        assertThat(relation.where().query(), isSQL("null"));
        assertEquals(relation.joinPairs().size(), 1);
        assertThat(relation.joinPairs().get(0).condition(),
                   isSQL("((doc.users.id = doc.users_multi_pk.id) AND (doc.users.name = doc.users_multi_pk.name))"));
    }

    @Test
    public void testNaturalJoinSyntax() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        analyze("select * from users natural join users_multi_pk");
    }

    @Test
    public void testInnerJoinSyntaxWithWhereClause() throws Exception {
        QueriedSelectRelation relation = analyze(
            "select * from users join users_multi_pk on users.id = users_multi_pk.id " +
            "where users.name = 'Arthur'");

        assertThat(relation.joinPairs().get(0).condition(),
            isSQL("(doc.users.id = doc.users_multi_pk.id)"));

        assertThat(relation.where().query(), isSQL("(doc.users.name = 'Arthur')"));
        AnalyzedRelation users = relation.from().get(0);
    }

    public void testSelfJoinSyntaxWithWhereClause() throws Exception {
        QueriedSelectRelation relation = analyze("select t2.id from users as t1 join users as t2 on t1.id = t2.id " +
                                                 "where t1.name = 'foo' and t2.name = 'bar'");

        assertThat(relation.where().queryOrFallback(),
                   isSQL("((t1.name = 'foo') AND (t2.name = 'bar'))"));
        assertThat(relation, instanceOf(QueriedSelectRelation.class));
    }

    @Test
    public void testJoinWithOrderBy() throws Exception {
        AnalyzedRelation relation = analyze("select users.id from users, users_multi_pk order by users.id");
        assertThat(relation, instanceOf(QueriedSelectRelation.class));

        QueriedSelectRelation mss = (QueriedSelectRelation) relation;

        assertThat(mss.orderBy(), isSQL("doc.users.id"));
    }

    @Test
    public void testJoinWithOrderByOnCount() throws Exception {
        AnalyzedRelation relation = analyze("select count(*) from users u1, users_multi_pk u2 " +
                                            "order by 1");
        QueriedSelectRelation mss = (QueriedSelectRelation) relation;
        assertThat(mss.orderBy(), isSQL("count(*)"));
    }

    @Test
    public void testJoinWithMultiRelationOrderBy() throws Exception {
        AnalyzedRelation relation = analyze(
            "select u1.id from users u1, users_multi_pk u2 order by u2.id, u1.name || u2.name");
        assertThat(relation, instanceOf(QueriedSelectRelation.class));

        QueriedSelectRelation mss = (QueriedSelectRelation) relation;
        AnalyzedRelation u1 = mss.from().iterator().next();
        assertThat(u1.outputs(), allOf(
            hasItem(isField("name")),
            hasItem(isField("id")))
        );
    }

    @Test
    public void testJoinConditionIsNotPartOfOutputs() throws Exception {
        AnalyzedRelation rel = analyze(
            "select u1.name from users u1 inner join users u2 on u1.id = u2.id order by u2.date");
        assertThat(rel.outputs(), contains(isField("name")));
    }

    @Test
    public void testUnionDistinct() {
        expectedException.expect(UnsupportedFeatureException.class);
        expectedException.expectMessage("UNION [DISTINCT] is not supported");
        analyze("select * from users union select * from users_multi_pk");
    }

    @Test
    public void testIntersect() {
        expectedException.expect(UnsupportedFeatureException.class);
        expectedException.expectMessage("INTERSECT is not supported");
        analyze("select * from users intersect select * from users_multi_pk");
    }

    @Test
    public void testExcept() {
        expectedException.expect(UnsupportedFeatureException.class);
        expectedException.expectMessage("EXCEPT is not supported");
        analyze("select * from users except select * from users_multi_pk");
    }

    @Test
    public void testArrayCompareInvalidArray() throws Exception {
        expectedException.expect(ConversionException.class);
        expectedException.expectMessage("Cannot cast `name` of type `text` to type `undefined_array`");
        analyze("select * from users where 'George' = ANY (name)");
    }

    @Test
    public void testArrayCompareObjectArray() throws Exception {
        QueriedSelectRelation relation = analyze("select * from users where {id=1} = ANY (friends)");
        assertThat(relation.where().queryOrFallback(), is(isFunction("any_=")));
    }

    @Test
    public void testArrayCompareAny() throws Exception {
        QueriedSelectRelation relation = analyze("select * from users where 0 = ANY (counters)");
        assertThat(relation.where().hasQuery(), is(true));

        FunctionInfo anyInfo = ((Function) relation.where().query()).info();
        assertThat(anyInfo.ident().name(), is("any_="));

        relation = analyze("select * from users where 0 = ANY (counters)");
        assertThat(relation.where().hasQuery(), is(true));

        anyInfo = ((Function) relation.where().query()).info();
        assertThat(anyInfo.ident().name(), is("any_="));
    }

    @Test
    public void testArrayCompareAnyNeq() throws Exception {
        QueriedSelectRelation relation = analyze("select * from users where 4.3 != ANY (counters)");
        assertThat(relation.where().hasQuery(), is(true));

        FunctionInfo anyInfo = ((Function) relation.where().query()).info();
        assertThat(anyInfo.ident().name(), is("any_<>"));
    }

    @Test
    public void testArrayCompareAll() throws Exception {
        QueriedSelectRelation relation = analyze("select * from users where 0 = ALL (counters)");
        assertThat(relation.where().query(), isFunction("_all_="));
    }

    @Test
    public void testImplicitContainmentOnObjectArrayFields() {
        // users.friends is an object array,
        // so its fields are selected as arrays,
        // ergo simple comparison does not work here
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot cast `friends['id']` of type `bigint_array` to type `bigint`");
        analyze("select * from users where 5 = friends['id']");
    }

    @Test
    public void testAnyOnObjectArrayField() throws Exception {
        QueriedSelectRelation relation = analyze(
            "select * from users where 5 = ANY (friends['id'])");
        assertThat(relation.where().hasQuery(), is(true));
        Function anyFunction = (Function) relation.where().query();
        assertThat(anyFunction.info().ident().name(), is(AnyOperators.Names.EQ));
        assertThat(anyFunction.arguments().get(1), isReference("friends['id']", new ArrayType<>(DataTypes.LONG)));
        assertThat(anyFunction.arguments().get(0), isLiteral(5L));
    }

    @Test
    public void testAnyOnArrayInObjectArray() throws Exception {
        QueriedSelectRelation relation = analyze(
            "select * from users where ['vogon lyric lovers'] = ANY (friends['groups'])");
        assertThat(
            relation.where().queryOrFallback(),
            isFunction(
                "any_=",
                isLiteral(
                    List.of("vogon lyric lovers"),
                    new ArrayType<>(DataTypes.STRING)),
                isReference("friends['groups']", new ArrayType<>(new ArrayType<>(DataTypes.STRING)))
            )
        );
    }

    @Test
    public void testTableAliasWrongUse() throws Exception {
        expectedException.expect(RelationUnknown.class);
        // caused by where users.awesome, would have to use where u.awesome = true instead
        expectedException.expectMessage("Relation 'doc.users' unknown");
        analyze("select * from users as u where users.awesome = true");
    }

    @Test
    public void testTableAliasFullQualifiedName() throws Exception {
        expectedException.expect(RelationUnknown.class);
        // caused by where users.awesome, would have to use where u.awesome = true instead
        expectedException.expectMessage("Relation 'doc.users' unknown");
        analyze("select * from users as u where doc.users.awesome = true");
    }

    @Test
    public void testAliasSubscript() throws Exception {
        AnalyzedRelation relation = analyze(
            "select u.friends['id'] from users as u");
        assertThat(relation.outputs().size(), is(1));
        Symbol s = relation.outputs().get(0);
        assertThat(s, notNullValue());
        assertThat(s, isFunction(SubscriptFunction.NAME, isField("friends"), isLiteral("id")));
    }

    @Test
    public void testOrderByWithOrdinal() throws Exception {
        QueriedSelectRelation relation = analyze("select name from users u order by 1");
        assertThat(
            relation.outputs(),
            equalTo(relation.orderBy().orderBySymbols())
        );
    }

    @Test
    public void testOrderByOnArray() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Cannot ORDER BY 'friends': invalid data type 'object_array'.");
        analyze("select * from users order by friends");
    }

    @Test
    public void testOrderByOnObject() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Cannot ORDER BY 'load': invalid data type 'object'.");
        analyze("select * from sys.nodes order by load");
    }

    @Test
    public void testArithmeticPlus() throws Exception {
        AnalyzedRelation relation = analyze("select load['1'] + load['5'] from sys.nodes");
        assertThat(((Function) relation.outputs().get(0)).info().ident().name(), is(ArithmeticFunctions.Names.ADD));
    }

    @Test
    public void testPrefixedNumericLiterals() throws Exception {
        AnalyzedRelation relation = analyze("select - - - 10");
        List<Symbol> outputs = relation.outputs();
        assertThat(outputs.get(0), is(Literal.of(-10L)));

        relation = analyze("select - + - 10");
        outputs = relation.outputs();
        assertThat(outputs.get(0), is(Literal.of(10L)));

        relation = analyze("select - (- 10 - + 10) * - (+ 10 + - 10)");
        outputs = relation.outputs();
        assertThat(outputs.get(0), is(Literal.of(0L)));
    }

    @Test
    public void testAnyLike() throws Exception {
        QueriedSelectRelation relation = analyze("select * from users where 'awesome' LIKE ANY (tags)");
        assertThat(relation.where().hasQuery(), is(true));
        Function query = (Function) relation.where().query();
        assertThat(query.info().ident().name(), is("any_like"));
        assertThat(query.arguments().size(), is(2));
        assertThat(query.arguments().get(0), instanceOf(Literal.class));
        assertThat(query.arguments().get(0), isLiteral("awesome", DataTypes.STRING));
        assertThat(query.arguments().get(1), isReference("tags"));
    }

    @Test
    public void testAnyLikeLiteralMatchAll() throws Exception {
        QueriedSelectRelation relation = analyze("select * from users where 'awesome' LIKE ANY (['a', 'b', 'awesome'])");
        assertThat(relation.where().queryOrFallback(), isLiteral(true));
    }

    @Test
    public void testAnyLikeLiteralNoMatch() throws Exception {
        QueriedSelectRelation relation = analyze("select * from users where 'awesome' LIKE ANY (['a', 'b'])");
        assertThat(relation.where().queryOrFallback(), isLiteral(false));
    }

    @Test
    public void testAnyNotLike() throws Exception {
        QueriedSelectRelation relation = analyze("select * from users where 'awesome' NOT LIKE ANY (tags)");
        assertThat(relation.where().hasQuery(), is(true));
        Function query = (Function) relation.where().query();
        assertThat(query.info().ident().name(), is("any_not_like"));

        assertThat(query.arguments().size(), is(2));
        assertThat(query.arguments().get(0), instanceOf(Literal.class));
        assertThat(query.arguments().get(0), isLiteral("awesome", DataTypes.STRING));
        assertThat(query.arguments().get(1), isReference("tags"));
    }

    @Test
    public void testAnyLikeInvalidArray() throws Exception {
        expectedException.expect(ConversionException.class);
        expectedException.expectMessage("Cannot cast `name` of type `text` to type `undefined_array`");
        analyze("select * from users where 'awesome' LIKE ANY (name)");
    }

    @Test
    public void testPositionalArgumentOrderByArrayType() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Cannot ORDER BY 'friends': invalid data type 'object_array'.");
        analyze("SELECT id, friends FROM users ORDER BY 2");
    }

    @Test
    public void testOrderByDistanceAlias() throws Exception {
        String stmt = "SELECT distance(loc, 'POINT(-0.1275 51.507222)') AS distance_to_london " +
                      "FROM locations " +
                      "ORDER BY distance_to_london";
        testDistanceOrderBy(stmt);
    }

    @Test
    public void testOrderByDistancePositionalArgument() throws Exception {
        String stmt = "SELECT distance(loc, 'POINT(-0.1275 51.507222)') " +
                      "FROM locations " +
                      "ORDER BY 1";
        testDistanceOrderBy(stmt);
    }

    @Test
    public void testOrderByDistanceExplicitly() throws Exception {
        String stmt = "SELECT distance(loc, 'POINT(-0.1275 51.507222)') " +
                      "FROM locations " +
                      "ORDER BY distance(loc, 'POINT(-0.1275 51.507222)')";
        testDistanceOrderBy(stmt);
    }

    @Test
    public void testOrderByDistancePermutatedExplicitly() throws Exception {
        String stmt = "SELECT distance('POINT(-0.1275 51.507222)', loc) " +
                      "FROM locations " +
                      "ORDER BY distance('POINT(-0.1275 51.507222)', loc)";
        testDistanceOrderBy(stmt);
    }

    private void testDistanceOrderBy(String stmt) throws Exception {
        QueriedSelectRelation relation = analyze(stmt);
        assertThat(relation.orderBy(), notNullValue());
        assertThat(
            relation.orderBy().orderBySymbols(),
            contains(
                anyOf(
                    isAlias("distance_to_london", isFunction(DistanceFunction.NAME)),
                    isFunction(DistanceFunction.NAME)
                )
            )
        );
    }

    @Test
    public void testWhereMatchOnColumn() throws Exception {
        QueriedSelectRelation relation = analyze("select * from users where match(name, 'Arthur Dent')");
        assertThat(relation.where().queryOrFallback(), Matchers.instanceOf(MatchPredicate.class));
        MatchPredicate matchPredicate = (MatchPredicate) relation.where().query();

        assertThat(matchPredicate.queryTerm(), isLiteral("Arthur Dent"));
        assertThat(matchPredicate.identBoostMap(), hasEntry(isReference("name"), isLiteral(null)));
        assertThat(matchPredicate.matchType(), is("best_fields"));
        assertThat(matchPredicate.options(), isLiteral(Map.of()));
    }

    @Test
    public void testMatchOnIndex() throws Exception {
        QueriedSelectRelation relation = analyze("select * from users where match(name_text_ft, 'Arthur Dent')");
        assertThat(relation.where().queryOrFallback(), instanceOf(MatchPredicate.class));
        MatchPredicate match = (MatchPredicate) relation.where().queryOrFallback();
        assertThat(match.identBoostMap(), hasEntry(isReference("name_text_ft"), isLiteral(null)));
        assertThat(match.queryTerm(), isLiteral("Arthur Dent"));
        assertThat(match.matchType(), is("best_fields"));
        assertThat(match.options(), isLiteral(Map.of()));
    }

    @Test
    public void testMatchOnDynamicColumn() throws Exception {
        expectedException.expect(ColumnUnknownException.class);
        expectedException.expectMessage("Column details['me_not_exizzt'] unknown");
        analyze("select * from users where match(details['me_not_exizzt'], 'Arthur Dent')");
    }

    @Test
    public void testMatchPredicateInResultColumnList() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("match predicate cannot be selected");
        analyze("select match(name, 'bar') from users");
    }

    @Test
    public void testMatchPredicateInGroupByClause() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("match predicate cannot be used in a GROUP BY clause");
        analyze("select count(*) from users group by MATCH(name, 'bar')");
    }

    @Test
    public void testMatchPredicateInOrderByClause() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("match predicate cannot be used in an ORDER BY clause");
        analyze("select name from users order by match(name, 'bar')");
    }

    @Test
    public void testMatchPredicateWithWrongQueryTerm() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot cast `[10, 20]` of type `bigint_array` to type `text`");
        analyze("select name from users order by match(name, [10, 20])");
    }

    @Test
    public void testSelectWhereSimpleMatchPredicate() throws Exception {
        QueriedSelectRelation relation = analyze("select * from users where match (text, 'awesome')");
        assertThat(relation.where().queryOrFallback(), instanceOf(MatchPredicate.class));
        MatchPredicate query = (MatchPredicate) relation.where().query();

        assertThat(query.identBoostMap(), hasEntry(isReference("text"), isLiteral(null)));
        assertThat(query.options(), isLiteral(Map.of()));
        assertThat(query.queryTerm(), isLiteral("awesome"));
        assertThat(query.matchType(), is("best_fields"));
    }

    @Test
    public void testSelectWhereFullMatchPredicate() throws Exception {
        QueriedSelectRelation relation = analyze(
            "select * from users " +
            "where match ((name 1.2, text), 'awesome') using best_fields with (analyzer='german')");
        Symbol query = relation.where().queryOrFallback();
        assertThat(query, instanceOf(MatchPredicate.class));
        MatchPredicate match = (MatchPredicate) query;
        assertThat(match.identBoostMap(), hasEntry(isReference("name"), isLiteral(1.2)));
        assertThat(match.identBoostMap(), hasEntry(isReference("text"), isLiteral(null)));
        assertThat(match.queryTerm(), isLiteral("awesome"));
        assertThat(match.matchType(), is("best_fields"));
        assertThat(match.options(), isLiteral(Map.of("analyzer", "german")));
    }

    @Test
    public void testWhereMatchUnknownType() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("invalid MATCH type 'some_fields'");
        analyze("select * from users " +
                "where match ((name 1.2, text), 'awesome') using some_fields");
    }

    @Test
    public void testUnknownSubscriptInSelectList() {
        expectedException.expect(ColumnUnknownException.class);
        expectedException.expectMessage("Column o['no_such_column'] unknown");
        analyze("select o['no_such_column'] from users");
    }

    @Test
    public void testUnknownSubscriptInQuery() {
        expectedException.expect(ColumnUnknownException.class);
        expectedException.expectMessage("Column o['no_such_column'] unknown");
        analyze("select * from users where o['no_such_column'] is not null");
    }

    @Test
    public void testWhereMatchAllowedTypes() throws Exception {
        QueriedSelectRelation best_fields_relation = analyze("select * from users " +
                                                        "where match ((name 1.2, text), 'awesome') using best_fields");
        QueriedSelectRelation most_fields_relation = analyze("select * from users " +
                                                        "where match ((name 1.2, text), 'awesome') using most_fields");
        QueriedSelectRelation cross_fields_relation = analyze("select * from users " +
                                                         "where match ((name 1.2, text), 'awesome') using cross_fields");
        QueriedSelectRelation phrase_relation = analyze("select * from users " +
                                                   "where match ((name 1.2, text), 'awesome') using phrase");
        QueriedSelectRelation phrase_prefix_relation = analyze("select * from users " +
                                                          "where match ((name 1.2, text), 'awesome') using phrase_prefix");

        assertThat(((MatchPredicate) best_fields_relation.where().query()).matchType(), is("best_fields"));
        assertThat(((MatchPredicate) most_fields_relation.where().query()).matchType(), is("most_fields"));
        assertThat(((MatchPredicate) cross_fields_relation.where().query()).matchType(), is("cross_fields"));
        assertThat(((MatchPredicate) phrase_relation.where().query()).matchType(), is("phrase"));
        assertThat(((MatchPredicate) phrase_prefix_relation.where().query()).matchType(), is("phrase_prefix"));
    }

    @Test
    public void testWhereMatchAllOptions() throws Exception {
        QueriedSelectRelation relation = analyze("select * from users " +
                                            "where match ((name 1.2, text), 'awesome') using best_fields with " +
                                            "(" +
                                            "  analyzer='german'," +
                                            "  boost=4.6," +
                                            "  tie_breaker=0.75," +
                                            "  operator='or'," +
                                            "  minimum_should_match=4," +
                                            "  fuzziness=12," +
                                            "  max_expansions=3," +
                                            "  prefix_length=4," +
                                            "  rewrite='constant_score_boolean'," +
                                            "  fuzzy_rewrite='top_terms_20'," +
                                            "  zero_terms_query='all'," +
                                            "  cutoff_frequency=5," +
                                            "  slop=3" +
                                            ")");
        MatchPredicate match = (MatchPredicate) relation.where().query();
        assertThat(match.options(), isLiteral(Map.ofEntries(
            Map.entry("analyzer", "german"),
            Map.entry("boost", 4.6),
            Map.entry("cutoff_frequency", 5L),
            Map.entry("fuzziness", 12L),
            Map.entry("fuzzy_rewrite", "top_terms_20"),
            Map.entry("max_expansions", 3L),
            Map.entry("minimum_should_match", 4L),
            Map.entry("operator", "or"),
            Map.entry("prefix_length", 4L),
            Map.entry("rewrite", "constant_score_boolean"),
            Map.entry("slop", 3L),
            Map.entry("tie_breaker", 0.75),
            Map.entry("zero_terms_query", "all")
        )));
    }

    @Test
    public void testHavingWithoutGroupBy() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("HAVING clause can only be used in GROUP BY or global aggregate queries");
        analyze("select * from users having max(bytes) > 100");
    }

    @Test
    public void testGlobalAggregateHaving() throws Exception {
        QueriedSelectRelation relation = analyze("select sum(floats) from users having sum(bytes) in (42, 43, 44)");
        Function havingFunction = (Function) relation.having().query();

        // assert that the in was converted to or
        assertThat(havingFunction.info().ident().name(), is(AnyOperators.Names.EQ));
    }

    @Test
    public void testGlobalAggregateReference() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot use column bytes outside of an Aggregation in HAVING clause. Only GROUP BY keys allowed here.");
        analyze("select sum(floats) from users having bytes in (42, 43, 44)");
    }


    @Test
    public void testScoreReferenceInvalidComparison() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("System column '_score' can only be used within a '>=' comparison without any surrounded predicate");
        analyze("select * from users where \"_score\" = 0.9");
    }

    @Test
    public void testScoreReferenceComparisonWithColumn() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("System column '_score' can only be used within a '>=' comparison without any surrounded predicate");
        analyze("select * from users where \"_score\" >= id::float");
    }

    @Test
    public void testScoreReferenceInvalidNotPredicate() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("System column '_score' can only be used within a '>=' comparison without any surrounded predicate");
        analyze("select * from users where not \"_score\" >= 0.9");
    }

    @Test
    public void testScoreReferenceInvalidLikePredicate() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("System column '_score' can only be used within a '>=' comparison without any surrounded predicate");
        analyze("select * from users where \"_score\" in (0.9)");
    }

    @Test
    public void testScoreReferenceInvalidNullPredicate() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("System column '_score' can only be used within a '>=' comparison without any surrounded predicate");
        analyze("select * from users where \"_score\" is null");
    }

    @Test
    public void testScoreReferenceInvalidNotNullPredicate() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("System column '_score' can only be used within a '>=' comparison without any surrounded predicate");
        analyze("select * from users where \"_score\" is not null");
    }


    @Test
    public void testRegexpMatchInvalidArg() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot cast `floats` of type `real` to type `text`");
        analyze("select * from users where floats ~ 'foo'");
    }

    @Test
    public void testRegexpMatchNull() throws Exception {
        QueriedSelectRelation relation = analyze("select * from users where name ~ null");
        assertThat(relation.where().queryOrFallback(), isLiteral(null));
    }

    @Test
    public void testRegexpMatch() throws Exception {
        QueriedSelectRelation relation = analyze("select * from users where name ~ '.*foo(bar)?'");
        assertThat(relation.where().hasQuery(), is(true));
        assertThat(((Function) relation.where().query()).info().ident().name(), is("op_~"));
    }

    @Test
    public void testSubscriptArray() throws Exception {
        AnalyzedRelation relation = analyze("select tags[1] from users");
        assertThat(relation.outputs().get(0), isFunction(SubscriptFunction.NAME));
        List<Symbol> arguments = ((Function) relation.outputs().get(0)).arguments();
        assertThat(arguments.size(), is(2));
        assertThat(arguments.get(0), isReference("tags"));
        assertThat(arguments.get(1), isLiteral(1));
    }

    @Test
    public void testSubscriptArrayInvalidIndexMin() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Array index must be in range 1 to 2147483648");
        analyze("select tags[0] from users");
    }

    @Test
    public void testSubscriptArrayInvalidIndexMax() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Array index must be in range 1 to 2147483648");
        analyze("select tags[2147483649] from users");
    }

    @Test
    public void testSubscriptArrayNested() throws Exception {
        AnalyzedRelation relation = analyze("select tags[1]['name'] from deeply_nested");
        assertThat(relation.outputs().get(0), isFunction(SubscriptFunction.NAME));
        List<Symbol> arguments = ((Function) relation.outputs().get(0)).arguments();
        assertThat(arguments.size(), is(2));
        assertThat(arguments.get(0), isReference("tags['name']"));
        assertThat(arguments.get(1), isLiteral(1));
    }

    @Test
    public void testSubscriptArrayInvalidNesting() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Nested array access is not supported");
        analyze("select tags[1]['metadata'][2] from deeply_nested");
    }

    @Test
    public void testSubscriptArrayAsAlias() throws Exception {
        AnalyzedRelation relation = analyze("select tags[1] as t_alias from users");
        assertThat(
            relation.outputs().get(0),
            isAlias(
                "t_alias",
                isFunction(SubscriptFunction.NAME, isReference("tags"), isLiteral(1))));
    }

    @Test
    public void testSubscriptArrayOnScalarResult() throws Exception {
        QueriedSelectRelation relation = analyze("select regexp_matches(name, '.*')[1] as t_alias from users order by t_alias");
        assertThat(
            relation.outputs(),
            contains(
                isAlias(
                    "t_alias",
                    isFunction(
                        SubscriptFunction.NAME,
                        isFunction("regexp_matches", isReference("name"), isLiteral(".*")),
                        isLiteral(1)
                    )
                )
            )
        );
        assertThat(
            relation.orderBy().orderBySymbols(),
            contains(
                isAlias("t_alias", isFunction(SubscriptFunction.NAME))
            )
        );
    }

    @Test
    public void testParameterSubcriptColumn() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Parameter substitution is not supported in subscript");
        analyze("select friends[?] from users");
    }

    @Test
    public void testParameterSubscriptLiteral() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Parameter substitution is not supported in subscript");
        analyze("select ['a','b','c'][?] from users");
    }

    @Test
    public void testArraySubqueryExpression() throws Exception {
        AnalyzedRelation relation = analyze("select array(select id from sys.shards) as shards_id_array from sys.shards");
        SelectSymbol arrayProjection = (SelectSymbol) ((AliasSymbol) relation.outputs().get(0)).symbol();
        assertThat(arrayProjection.getResultType(), is(SelectSymbol.ResultType.SINGLE_COLUMN_MULTIPLE_VALUES));
        assertThat(arrayProjection.valueType().id(), is(ArrayType.ID));
    }

    @Test
    public void testArraySubqueryWithMultipleColsThrowsUnsupportedSubExpression() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Subqueries with more than 1 column are not supported");
        analyze("select array(select id, num_docs from sys.shards) as tmp from sys.shards");
    }

    @Test
    public void testCastExpression() {
        AnalyzedRelation relation = analyze("select cast(other_id as text) from users");
        assertThat(relation.outputs().get(0),
            isFunction("to_text", List.of(DataTypes.LONG, DataTypes.UNDEFINED)));

        relation = analyze("select cast(1+1 as string) from users");
        assertThat(relation.outputs().get(0), isLiteral("2", DataTypes.STRING));

        relation = analyze("select cast(friends['id'] as array(text)) from users");
        assertThat(
            relation.outputs().get(0),
            isFunction(
                "to_text_array",
                List.of(new ArrayType<>(DataTypes.LONG), DataTypes.UNDEFINED)));
    }

    @Test
    public void testTryCastExpression() {
        AnalyzedRelation relation = analyze("select try_cast(other_id as text) from users");
        assertThat(relation.outputs().get(0), isFunction(
            "try_to_text", List.of(DataTypes.LONG, DataTypes.UNDEFINED)));

        relation = analyze("select try_cast(1+1 as string) from users");
        assertThat(relation.outputs().get(0), isLiteral("2", DataTypes.STRING));

        relation = analyze("select try_cast(null as string) from users");
        assertThat(relation.outputs().get(0), isLiteral(null, DataTypes.STRING));

        relation = analyze("select try_cast(counters as array(boolean)) from users");
        assertThat(
            relation.outputs().get(0),
            isFunction(
                "try_to_boolean_array",
                List.of(new ArrayType<>(DataTypes.LONG), DataTypes.UNDEFINED)));
    }

    @Test
    public void testTryCastReturnNullWhenCastFailsOnLiterals() {
        AnalyzedRelation relation = analyze("select try_cast('124123asdf' as integer) from users");
        assertThat(relation.outputs().get(0), isLiteral(null));

        relation = analyze("select try_cast(['fd', '3', '5'] as array(integer)) from users");
        assertThat(relation.outputs().get(0), isLiteral(null));

        relation = analyze("select try_cast('1' as boolean) from users");
        assertThat(relation.outputs().get(0), isLiteral(null));
    }

    @Test
    public void testSelectWithAliasRenaming() throws Exception {
        AnalyzedRelation relation = analyze("select text as name, name as n from users");
        assertThat(
            relation.outputs(),
            contains(
                isAlias("name", isReference("text")),
                isAlias("n", isReference("name"))
            ));
    }

    @Test
    public void testFunctionArgumentsCantBeAliases() throws Exception {
        expectedException.expect(ColumnUnknownException.class);
        expectedException.expectMessage("Column n unknown");
        analyze("select name as n, substr(n, 1, 1) from users");
    }

    @Test
    public void testSubscriptOnAliasShouldNotWork() throws Exception {
        expectedException.expect(ColumnUnknownException.class);
        expectedException.expectMessage("Column n unknown");
        analyze("select name as n, n[1] from users");
    }

    @Test
    public void testCanSelectColumnWithAndWithoutSubscript() throws Exception {
        AnalyzedRelation relation = analyze("select counters, counters[1] from users");
        Symbol counters = relation.outputs().get(0);
        Symbol countersSubscript = relation.outputs().get(1);

        assertThat(counters, isReference("counters"));
        assertThat(countersSubscript, isFunction("subscript"));
    }

    @Test
    public void testOrderByOnAliasWithSameColumnNameInSchema() throws Exception {
        // name exists in the table but isn't selected so not ambiguous
        QueriedSelectRelation relation = analyze("select other_id as name from users order by name");
        assertThat(relation.outputs(), contains(isAlias("name", isReference("other_id"))));
        assertThat(relation.orderBy().orderBySymbols(), contains(isAlias("name", isReference("other_id"))));
    }

    @Test
    public void testSelectPartitionedTableOrderBy() throws Exception {
        QueriedSelectRelation relation = analyze(
            "select id from multi_parted order by id, abs(num)");
        List<Symbol> symbols = relation.orderBy().orderBySymbols();
        assert symbols != null;
        assertThat(symbols.size(), is(2));
        assertThat(symbols.get(0), isReference("id"));
        assertThat(symbols.get(1), isFunction("abs"));
    }

    @Test
    public void testExtractFunctionWithLiteral() {
        AnalyzedRelation relation = analyze("select extract('day' from '2012-03-24') from users");
        Symbol symbol = relation.outputs().get(0);
        assertThat(symbol, isLiteral(24));
    }

    @Test
    public void testExtractFunctionWithWrongType() {
        AnalyzedRelation relation = analyze(
            "select extract(day from name::timestamp with time zone) from users");
        Symbol symbol = relation.outputs().get(0);
        assertThat(symbol, isFunction("extract_DAY_OF_MONTH"));

        Symbol argument = ((Function) symbol).arguments().get(0);
        assertThat(argument, isFunction("to_timestamp with time zone"));
    }

    @Test
    public void testExtractFunctionWithCorrectType() {
        AnalyzedRelation relation = analyze("select extract(day from timestamp) from transactions");

        Symbol symbol = relation.outputs().get(0);
        assertThat(symbol, isFunction("extract_DAY_OF_MONTH"));

        Symbol argument = ((Function) symbol).arguments().get(0);
        assertThat(argument, isReference("timestamp"));
    }

    @Test
    public void selectCurrentTimeStamp() {
        AnalyzedRelation relation = analyze("select CURRENT_TIMESTAMP from sys.cluster");
        assertThat(
            relation.outputs(),
            contains(
                isFunction("current_timestamp", isLiteral(3))
            )
        );
    }

    @Test
    public void testAnyRightLiteral() throws Exception {
        QueriedSelectRelation relation = analyze("select id from sys.shards where id = any ([1,2])");
        WhereClause whereClause = relation.where();
        assertThat(whereClause.hasQuery(), is(true));
        assertThat(whereClause.query(),
                   isFunction("any_=", List.of(DataTypes.INTEGER, new ArrayType<>(DataTypes.INTEGER))));
    }

    @Test
    public void testNonDeterministicFunctionsAreNotAllocated() throws Exception {
        QueriedSelectRelation relation = analyze(
            "select random(), random(), random() " +
            "from transactions " +
            "where random() = 13.2 " +
            "order by 1, random(), random()");
        List<Symbol> outputs = relation.outputs();
        List<Symbol> orderBySymbols = relation.orderBy().orderBySymbols();

        // non deterministic, all equal
        assertThat(outputs.get(0),
            allOf(
                equalTo(outputs.get(2)),
                equalTo(orderBySymbols.get(1))
            )
        );
        // different instances
        assertThat(outputs.get(0), allOf(
            not(sameInstance(outputs.get(2))),
            not(sameInstance(orderBySymbols.get(1))
            )));
        assertThat(outputs.get(1),
            equalTo(orderBySymbols.get(2)));

        // "order by 1" references output 1, its the same
        assertThat(outputs.get(0), is(equalTo(orderBySymbols.get(0))));
        assertThat(outputs.get(0), is(sameInstance(orderBySymbols.get(0))));
        assertThat(orderBySymbols.get(0), is(equalTo(orderBySymbols.get(1))));

        // check where clause
        WhereClause whereClause = relation.where();
        Function eqFunction = (Function) whereClause.query();
        Symbol whereClauseSleepFn = eqFunction.arguments().get(0);
        assertThat(outputs.get(0), is(equalTo(whereClauseSleepFn)));
    }

    @Test
    public void testSelectSameTableTwice() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("\"doc.users\" specified more than once in the FROM clause");
        analyze("select * from users, users");
    }

    @Test
    public void testSelectSameTableTwiceWithAndWithoutSchemaName() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("\"doc.users\" specified more than once in the FROM clause");
        analyze("select * from doc.users, users");
    }

    @Test
    public void testSelectSameTableTwiceWithSchemaName() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("\"sys.nodes\" specified more than once in the FROM clause");
        analyze("select * from sys.nodes, sys.nodes");
    }

    @Test
    public void testStarToFieldsInMultiSelect() throws Exception {
        AnalyzedRelation relation = analyze(
            "select jobs.stmt, operations.* from sys.jobs, sys.operations where jobs.id = operations.job_id");
        List<Symbol> joinOutputs = relation.outputs();

        AnalyzedRelation operations = analyze("select * from sys.operations");
        List<Symbol> operationOutputs = operations.outputs();
        assertThat(joinOutputs.size(), is(operationOutputs.size() + 1));
    }

    @Test
    public void testSelectStarWithInvalidPrefix() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("The relation \"foo\" is not in the FROM clause.");
        analyze("select foo.* from sys.operations");
    }

    @Test
    public void testFullQualifiedStarPrefix() throws Exception {
        AnalyzedRelation relation = analyze("select sys.jobs.* from sys.jobs");
        List<Symbol> outputs = relation.outputs();
        assertThat(outputs.size(), is(5));
        //noinspection unchecked
        assertThat(outputs, Matchers.contains(isReference("id"),
            isReference("node"),
            isReference("started"),
            isReference("stmt"),
            isReference("username"))
        );
    }

    @Test
    public void testFullQualifiedStarPrefixWithAliasForTable() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("The relation \"sys.operations\" is not in the FROM clause.");
        analyze("select sys.operations.* from sys.operations t1");
    }

    @Test
    public void testSelectStarWithTableAliasAsPrefix() throws Exception {
        AnalyzedRelation relation = analyze("select t1.* from sys.jobs t1");
        List<Symbol> outputs = relation.outputs();
        assertThat(outputs.size(), is(5));
        assertThat(outputs, Matchers.contains(
            isField("id"),
            isField("node"),
            isField("started"),
            isField("stmt"),
            isField("username"))
        );
    }

    @Test
    public void testAmbiguousStarPrefix() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("The referenced relation \"users\" is ambiguous.");
        analyze("select users.* from doc.users, foo.users");
    }

    @Test
    public void testSelectMatchOnGeoShape() throws Exception {
        QueriedSelectRelation relation = analyze(
            "select * from users where match(shape, 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))')");
        assertThat(relation.where().query(), Matchers.instanceOf(MatchPredicate.class));
    }

    @Test
    public void testSelectMatchOnGeoShapeObjectLiteral() throws Exception {
        QueriedSelectRelation relation = analyze(
            "select * from users where match(shape, {type='Polygon', coordinates=[[[30, 10], [40, 40], [20, 40], [10, 20], [30, 10]]]})");
        assertThat(relation.where().query(), Matchers.instanceOf(MatchPredicate.class));
    }

    @Test
    public void testOrderByGeoShape() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Cannot ORDER BY 'shape': invalid data type 'geo_shape'.");
        analyze("select * from users ORDER BY shape");
    }

    @Test
    public void testSelectStarFromUnnest() throws Exception {
        AnalyzedRelation relation = analyze("select * from unnest([1, 2], ['Marvin', 'Trillian'])");
        //noinspection generics
        assertThat(relation.outputs(), contains(isReference("col1"), isReference("col2")));
    }

    @Test
    public void testSelectStarFromUnnestWithInvalidArguments() throws Exception {
        expectedException.expect(ConversionException.class);
        expectedException.expectMessage("Cannot cast `1` of type `bigint` to type `undefined_array`");
        analyze("select * from unnest(1, 'foo')");
    }

    @Test
    public void testSelectCol1FromUnnest() throws Exception {
        AnalyzedRelation relation = analyze("select col1 from unnest([1, 2], ['Marvin', 'Trillian'])");
        assertThat(relation.outputs(), contains(isReference("col1")));
    }

    @Test
    public void testCollectSetCanBeUsedInHaving() throws Exception {
        QueriedSelectRelation relation = analyze(
            "select collect_set(recovery['size']['percent']), schema_name, table_name " +
            "from sys.shards " +
            "group by 2, 3 " +
            "having collect_set(recovery['size']['percent']) != [100.0] " +
            "order by 2, 3");
        assertThat(relation.having(), notNullValue());
        assertThat(relation.having().query(),
            isSQL("(NOT (collect_set(sys.shards.recovery['size']['percent']) = [100.0]))"));
    }

    @Test
    public void testNegationOfNonNumericLiteralsShouldFail() throws Exception {
        expectedException.expectMessage("Cannot negate 'foo'. You may need to add explicit type casts");
        analyze("select - 'foo'");
    }

    @Test
    public void testMatchInExplicitJoinConditionIsProhibited() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot use MATCH predicates on columns of 2 different relations");
        analyze("select * from users u1 inner join users u2 on match((u1.name, u2.name), 'foo')");
    }

    @Test
    public void testUnnestWithMoreThat10Columns() {
        AnalyzedRelation relation =
            analyze("select * from unnest(['a'], ['b'], [0], [0], [0], [0], [0], [0], [0], [0], [0])");

        String sqlFields = "col1, col2, col3, col4, " +
                           "col5, col6, col7, col8, " +
                           "col9, col10, col11";
        assertThat(relation.outputs(), contains(
            isReference("col1"),
            isReference("col2"),
            isReference("col3"),
            isReference("col4"),
            isReference("col5"),
            isReference("col6"),
            isReference("col7"),
            isReference("col8"),
            isReference("col9"),
            isReference("col10"),
            isReference("col11")
        ));
    }

    @Test
    public void testUnnestWithObjectColumn() {
        QueriedSelectRelation rel = analyze("select col1['x'] from unnest([{x=1}])");
        assertThat(rel.outputs(), contains(isFunction("subscript_obj", isReference("col1"), isLiteral("x"))));
    }

    @Test
    public void testScalarCanBeUsedInFromClause() {
        QueriedSelectRelation relation = analyze("select * from abs(1)");
        assertThat(relation.outputs(), contains(isReference("abs")));
        assertThat(relation.from().get(0), instanceOf(TableFunctionRelation.class));
    }

    @Test
    public void testCannotUseSameTableNameMoreThanOnce() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("\"abs\" specified more than once in the FROM clause");
        analyze("select * from abs(1), abs(5)");
    }

    @Test
    public void testWindowFunctionCannotBeUsedInFromClause() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Window or Aggregate function: 'row_number' is not allowed in function in FROM clause");
        analyze("select * from row_number()");
    }

    @Test
    public void testAggregateCannotBeUsedInFromClause() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Window or Aggregate function: 'count' is not allowed in function in FROM clause");
        analyze("select * from count()");
    }

    @Test
    public void testSubSelectWithAccessToParentRelationThrowsUnsupportedFeature() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Cannot use relation \"doc.t1\" in this context. It is only accessible in the parent context");
        analyze("select (select 1 from t1 as ti where ti.x = t1.x) from t1");
    }

    @Test
    public void testSubSelectWithAccessToParentRelationAliasThrowsUnsupportedFeature() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Cannot use relation \"tparent\" in this context. It is only accessible in the parent context");
        analyze("select (select 1 from t1 where t1.x = tparent.x) from t1 as tparent");
    }

    @Test
    public void testSubSelectWithAccessToGrandParentRelation() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Cannot use relation \"grandparent\" in this context. It is only accessible in the parent context");
        analyze("select (select (select 1 from t1 where grandparent.x = t1.x) from t1 as parent) from t1 as grandparent");
    }

    @Test
    public void testCustomSchemaSubSelectWithAccessToParentRelation() throws Exception {
        SQLExecutor sqlExecutor2 = SQLExecutor.builder(clusterService)
            .setSearchPath("foo")
            .addTable("create table foo.t1 (id bigint primary key, name text)")
            .build();
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Cannot use relation \"foo.t1\" in this context. It is only accessible in the parent context");
        sqlExecutor2.analyze("select * from t1 where id = (select 1 from t1 as x where x.id = t1.id)");
    }

    @Test
    public void testContextForExplicitJoinsPrecedesImplicitJoins() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Cannot use relation \"doc.t1\" in this context. It is only accessible in the parent context");
        // Inner join has to be processed before implicit cross join.
        // Inner join does not know about t1's fields (!)
        analyze("select * from t1, t2 inner join t1 b on b.x = t1.x");
    }

    @Test
    public void testColumnOutputWithSingleRowSubselect() {
        AnalyzedRelation relation = analyze("select 1 = \n (select \n 2\n)\n");
        assertThat(relation.outputs(), contains(
            isFunction("op_=", isLiteral(1L), instanceOf(SelectSymbol.class)))
        );
    }

    @Test
    public void testTableAliasIsNotAddressableByColumnNameWithSchema() {
        expectedException.expectMessage("Relation 'doc.a' unknown");
        analyze("select doc.a.x from t1 as a");
    }

    @Test
    public void testUsingTableFunctionInGroupByIsProhibited() {
        expectedException.expectMessage("Table functions are not allowed in GROUP BY");
        analyze("select count(*) from t1 group by unnest([1])");
    }

    @Test
    public void testUsingTableFunctionInHavingIsProhibited() {
        expectedException.expectMessage("Table functions are not allowed in HAVING");
        analyze("select count(*) from t1 having unnest([1]) > 1");
    }

    @Test
    public void testUsingTableFunctionInWhereClauseIsNotAllowed() {
        expectedException.expectMessage("Table functions are not allowed in WHERE");
        analyze("select * from sys.nodes where unnest([1]) = 1");
    }


    public void testUsingWindowFunctionInGroupByIsProhibited() {
        expectedException.expectMessage("Window functions are not allowed in GROUP BY");
        analyze("select count(*) from t1 group by sum(1) OVER()");
    }

    @Test
    public void testUsingWindowFunctionInHavingIsProhibited() {
        expectedException.expectMessage("Window functions are not allowed in HAVING");
        analyze("select count(*) from t1 having sum(1) OVER() > 1");
    }

    @Test
    public void testUsingWindowFunctionInWhereClauseIsNotAllowed() {
        expectedException.expectMessage("Window functions are not allowed in WHERE");
        analyze("select count(*) from t1 where sum(1) OVER() = 1");
    }

    @Test
    public void testCastToNestedArrayCanBeUsed() {
        AnalyzedRelation relation = analyze("select [[1, 2, 3]]::array(array(int))");
        assertThat(relation.outputs().get(0).valueType(), is(new ArrayType(new ArrayType(DataTypes.INTEGER))));
    }

    @Test
    public void testCastTimestampFromStringLiteral()  {
        AnalyzedRelation relation = analyze("select timestamp '2018-12-12T00:00:00'");
        assertThat(relation.outputs().get(0).valueType(), is(DataTypes.TIMESTAMPZ));
    }

    @Test
    public void testCastTimestampWithoutTimeZoneFromStringLiteralUsingSQLStandardFormat()  {
        AnalyzedRelation relation = analyze("select timestamp without time zone '2018-12-12 00:00:00'");
        assertThat(relation.outputs().get(0).valueType(), is(DataTypes.TIMESTAMP));
    }

    @Test
    public void test_element_within_object_array_of_derived_table_can_be_accessed_using_subscript() {
        QueriedSelectRelation relation = analyze("select s.friends['id'] from (select friends from doc.users) s");
        assertThat(
            relation.outputs(),
            contains(isFunction(SubscriptFunction.NAME, isField("friends"), isLiteral("id")))
        );
    }

    @Test
    public void test_can_access_element_within_object_array_of_derived_table_containing_a_join() {
        AnalyzedRelation relation = analyze("select joined.f1['id'], joined.f2['id'] from " +
                "(select u1.friends as f1, u2.friends as f2 from doc.users u1, doc.users u2) joined");
        assertThat(relation.outputs(), contains(
            isFunction(SubscriptFunction.NAME, isField("f1"), isLiteral("id")),
            isFunction(SubscriptFunction.NAME, isField("f2"), isLiteral("id"))
        ));
    }

    @Test
    public void test_can_access_element_within_object_array_of_derived_table_containing_a_join_with_ambiguous_column_name() {
        expectedException.expect(AmbiguousColumnException.class);
        expectedException.expectMessage("Column \"friends\" is ambiguous");
        analyze("select joined.friends['id'] from " +
                "(select u1.friends, u2.friends from doc.users u1, doc.users u2) joined");
    }

    @Test
    public void test_can_access_element_within_object_array_of_derived_table_containing_a_union() {
        AnalyzedRelation relation = analyze("select joined.f1['id'] from" +
                "  (select friends as f1 from doc.users u1 " +
                "   union all" +
                "   select friends from doc.users u2) as joined");
        assertThat(relation.outputs(), contains(
            isFunction(
                SubscriptFunction.NAME,
                isField("f1"),
                isLiteral("id")
            )
       ));
    }

    @Test
    public void test_select_from_unknown_schema_has_suggestion_for_correct_schema() {
        expectedException.expectMessage("Schema 'Doc' unknown. Maybe you meant 'doc'");
        analyze("select * from \"Doc\".users");
    }

    @Test
    public void test_select_from_unkown_table_has_suggestion_for_correct_table() {
        expectedException.expectMessage("Relation 'uusers' unknown. Maybe you meant 'users'");
        analyze("select * from uusers");
    }

    @Test
    public void test_select_from_unkown_table_has_suggestion_for_similar_tables() {
        expectedException.expectMessage("Relation 'foobar' unknown. Maybe you meant one of: fooobar, \"Foobaarr\"");
        analyze("select * from foobar");
    }

    @Test
    public void test_nested_column_of_object_can_be_selected_using_composite_type_access_syntax() {
        AnalyzedRelation relation = analyze("select (address).postcode from users");
        assertThat(relation.outputs(), contains(isReference("address['postcode']")));
    }

    @Test
    public void test_deep_nested_column_of_object_can_be_selected_using_composite_type_access_syntax() {
        AnalyzedRelation relation = analyze("select ((details).stuff).name from deeply_nested");
        assertThat(relation.outputs(), contains(isReference("details['stuff']['name']")));
    }

    @Test
    public void test_record_subscript_syntax_can_be_used_on_object_literals() {
        AnalyzedRelation rel = analyze("select ({x=10}).x");
        assertThat(
            rel.outputs(),
            contains(isFunction("subscript_obj", isLiteral(Map.of("x", 10L)), isLiteral("x")))
        );
    }

    @Test
    public void test_table_function_with_multiple_columns_in_select_list_has_row_type() {
        AnalyzedRelation rel = analyze("select unnest([1, 2], [3, 4])");
        assertThat(rel.outputs().get(0).valueType().getName(), is("record"));
    }
}
