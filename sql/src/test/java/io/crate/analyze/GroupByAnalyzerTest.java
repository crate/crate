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

package io.crate.analyze;

import com.google.common.collect.ImmutableMap;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.relations.RelationAnalyzer;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.udf.UserDefinedFunctionService;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.doc.DocTableInfoFactory;
import io.crate.metadata.doc.TestingDocTableInfoFactory;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;
import org.elasticsearch.common.inject.Provider;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static io.crate.analyze.TableDefinitions.SHARD_ROUTING;
import static io.crate.testing.SymbolMatchers.isField;
import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isLiteral;
import static io.crate.testing.SymbolMatchers.isReference;
import static io.crate.testing.TestingHelpers.getFunctions;
import static io.crate.testing.TestingHelpers.isSQL;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

@SuppressWarnings("ConstantConditions")
public class GroupByAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor sqlExecutor;
    private Provider<RelationAnalyzer> analyzerProvider = () -> null;

    @Before
    public void prepare() {
        DocTableInfo fooUserTableInfo = TestingTableInfo.builder(new TableIdent("foo", "users"), SHARD_ROUTING)
            .add("id", DataTypes.LONG, null)
            .add("name", DataTypes.STRING, null)
            .add("age", DataTypes.INTEGER, null)
            .add("height", DataTypes.INTEGER, null)
            .addPrimaryKey("id")
            .build();
        DocTableInfoFactory fooTableFactory = new TestingDocTableInfoFactory(
            ImmutableMap.of(fooUserTableInfo.ident(), fooUserTableInfo));
        Functions functions = getFunctions();
        UserDefinedFunctionService udfService = new UserDefinedFunctionService(clusterService, functions);
        sqlExecutor = SQLExecutor.builder(clusterService)
            .enableDefaultTables()
            .addSchema(new DocSchemaInfo("foo", clusterService, functions, udfService, (ident, state) -> null, fooTableFactory))
            .build();
    }

    private <T extends AnalyzedStatement> T analyze(String statement) {
        return sqlExecutor.analyze(statement);
    }

    @Test
    public void testGroupBySubscriptMissingOutput() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("column 'load['5']' must appear in the GROUP BY clause or be used in an aggregation function");
        analyze("select load['5'] from sys.nodes group by load['1']");
    }

    public void testGroupKeyNotInResultColumnList() throws Exception {
        QueriedRelation relation = analyze("select count(*) from sys.nodes group by name");
        assertThat(relation.querySpec().groupBy().size(), is(1));
        assertThat(relation.fields().get(0).path().outputName(), is("count(*)"));
    }

    @Test
    public void testGroupByOnAlias() throws Exception {
        QueriedRelation relation = analyze("select count(*), name as n from sys.nodes group by n");
        assertThat(relation.querySpec().groupBy().size(), is(1));
        assertThat(relation.fields().get(0).path().outputName(), is("count(*)"));
        assertThat(relation.fields().get(1).path().outputName(), is("n"));

        assertEquals(relation.querySpec().groupBy().get(0), relation.querySpec().outputs().get(1));
    }

    @Test
    public void testGroupByOnOrdinal() throws Exception {
        // just like in postgres access by ordinal starts with 1
        QueriedRelation relation = analyze("select count(*), name as n from sys.nodes group by 2");
        assertThat(relation.querySpec().groupBy().size(), is(1));
        assertEquals(relation.querySpec().groupBy().get(0), relation.querySpec().outputs().get(1));
    }

    @Test
    public void testGroupByOnOrdinalAggregation() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Aggregate functions are not allowed in GROUP BY");
        analyze("select count(*), name as n from sys.nodes group by 1");
    }

    @Test
    public void testGroupByWithDistinctAggregation() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Aggregate functions are not allowed in GROUP BY");
        analyze("select count(DISTINCT name) from sys.nodes group by 1");
    }

    @Test
    public void testGroupByScalarAliasedWithRealColumnNameFailsIfScalarColumnIsNotGrouped() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(
            "column 'height' must appear in the GROUP BY clause or be used in an aggregation function");
        analyze("select 1/height as age from foo.users group by age");
    }

    @Test
    public void testGroupByScalarAliasAndValueInScalar() {
        QueriedRelation relation =
            analyze("select 1/age as age from foo.users group by age order by age");
        assertThat(relation.querySpec().groupBy().isEmpty(), is(false));
        List<Symbol> groupBySymbols = relation.querySpec().groupBy();
        assertThat(((Reference) groupBySymbols.get(0)).ident().columnIdent().fqn(), is("age"));
    }

    @Test
    public void testGroupByScalarAlias() {
        // grouping by what's under the alias, the 1/age values
        QueriedRelation relation = analyze("select 1/age as theAlias from foo.users group by theAlias");
        assertThat(relation.querySpec().groupBy().isEmpty(), is(false));
        List<Symbol> groupBySymbols = relation.querySpec().groupBy();
        Symbol groupBy = groupBySymbols.get(0);
        assertThat(groupBy, instanceOf(Function.class));
        Function groupByFunction = (Function) groupBy;
        assertThat(((Reference) groupByFunction.arguments().get(1)).ident().columnIdent().fqn(), is("age"));
    }

    @Test
    public void testGroupByColumnInScalar() {
        // grouping by height values
        QueriedRelation relation = analyze("select 1/age as height from foo.users group by age");
        assertThat(relation.querySpec().groupBy().isEmpty(), is(false));
        List<Symbol> groupBySymbols = relation.querySpec().groupBy();
        assertThat(((Reference) groupBySymbols.get(0)).ident().columnIdent().fqn(), is("age"));
    }

    @Test
    public void testGroupByScalar() {
        QueriedRelation relation = analyze("select 1/age from foo.users group by 1/age;");
        assertThat(relation.querySpec().groupBy().isEmpty(), is(false));
        List<Symbol> groupBySymbols = relation.querySpec().groupBy();
        Symbol groupBy = groupBySymbols.get(0);
        assertThat(groupBy, instanceOf(Function.class));
    }

    @Test
    public void testGroupByAliasedLiteral() {
        QueriedRelation relation = analyze("select 58 as fiftyEight from foo.users group by fiftyEight;");
        assertThat(relation.querySpec().groupBy().isEmpty(), is(false));
        List<Symbol> groupBySymbols = relation.querySpec().groupBy();
        assertThat(groupBySymbols.get(0).symbolType().isValueSymbol(), is(true));
    }

    @Test
    public void testGroupByLiteralAliasedWithRealColumnNameGroupsByColumnValue() {
        QueriedRelation relation = analyze("select 58 as age from foo.users group by age;");
        assertThat(relation.querySpec().groupBy().isEmpty(), is(false));
        List<Symbol> groupBySymbols = relation.querySpec().groupBy();
        ReferenceIdent groupByIdent = ((Reference) groupBySymbols.get(0)).ident();
        assertThat(groupByIdent.columnIdent().fqn(), is("age"));
        assertThat(groupByIdent.tableIdent().fqn(), is("foo.users"));
    }

    @Test
    public void testNegateAliasRealColumnGroupByAlias() {
        QueriedRelation relation = analyze("select age age, - age age from foo.users group by age;");
        assertThat(relation.querySpec().groupBy().isEmpty(), is(false));
        List<Symbol> groupBySymbols = relation.querySpec().groupBy();
        ReferenceIdent groupByIdent = ((Reference) groupBySymbols.get(0)).ident();
        assertThat(groupByIdent.columnIdent().fqn(), is("age"));
        assertThat(groupByIdent.tableIdent().fqn(), is("foo.users"));
    }

    @Test
    public void testGroupBySubscript() throws Exception {
        QueriedRelation relation = analyze("select load['1'], count(*) from sys.nodes group by load['1']");
        assertThat(relation.querySpec().limit(), nullValue());

        assertThat(relation.querySpec().groupBy(), notNullValue());
        assertThat(relation.querySpec().outputs().size(), is(2));
        assertThat(relation.querySpec().groupBy().size(), is(1));
        assertThat(relation.querySpec().groupBy().get(0), isReference("load['1']"));
    }

    @Test
    public void testSelectGroupByOrderByWithColumnMissingFromSelect() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("ORDER BY expression 'id' must appear in the select clause " +
                                        "when grouping or global aggregation is used");
        analyze("select name, count(id) from users group by name order by id");
    }

    @Test
    public void testSelectGroupByOrderByWithAggregateFunctionInOrderByClause() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("ORDER BY function 'max(count(upper(name)))' is not allowed. " +
                                        "Only scalar functions can be used");
        analyze("select name, count(id) from users group by name order by max(count(upper(name)))");
    }

    @Test
    public void testSelectAggregationMissingGroupBy() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(
            "column 'name' must appear in the GROUP BY clause or be used in an aggregation function");
        analyze("select name, count(id) from users");
    }

    @Test
    public void testSelectGlobalDistinctAggregationMissingGroupBy() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(
            "column 'name' must appear in the GROUP BY clause or be used in an aggregation function");
        analyze("select distinct name, count(id) from users");
    }


    @Test
    public void testSelectDistinctWithGroupBy() {
        QueriedRelation relation = analyze("select distinct max(id) from users group by name order by 1");
        assertThat(relation, instanceOf(QueriedSelectRelation.class));
        assertThat(relation.querySpec(),
            isSQL("SELECT doc.users.max(id) GROUP BY doc.users.max(id) ORDER BY doc.users.max(id)"));
        QueriedSelectRelation outerRelation = (QueriedSelectRelation) relation;
        assertThat(outerRelation.subRelation(), instanceOf(QueriedDocTable.class));
        assertThat(outerRelation.subRelation().querySpec(),
            isSQL("SELECT max(doc.users.id) GROUP BY doc.users.name"));
    }

    @Test
    public void testSelectDistinctWithGroupByLimitAndOffset() {
        QueriedRelation relation =
            analyze("select distinct max(id) from users group by name order by 1 limit 5 offset 10");
        assertThat(relation, instanceOf(QueriedSelectRelation.class));
        assertThat(relation.querySpec(),
            isSQL("SELECT doc.users.max(id) GROUP BY doc.users.max(id) " +
                  "ORDER BY doc.users.max(id) LIMIT 5 OFFSET 10"));
        QueriedSelectRelation outerRelation = (QueriedSelectRelation) relation;
        assertThat(outerRelation.subRelation(), instanceOf(QueriedDocTable.class));
        assertThat(outerRelation.subRelation().querySpec(),
            isSQL("SELECT max(doc.users.id) GROUP BY doc.users.name"));
    }

    @Test
    public void testSelectDistinctWithGroupByOnJoin() {
        QueriedRelation relation =
            analyze("select DISTINCT max(users.id) from users " +
                    "  inner join users_multi_pk on users.id = users_multi_pk.id " +
                    "group by users.name order by 1");
        assertThat(relation, instanceOf(QueriedSelectRelation.class));
        assertThat(relation.querySpec(),
            isSQL("SELECT MultiSourceSelect.max(id) " +
                  "GROUP BY MultiSourceSelect.max(id) " +
                  "ORDER BY MultiSourceSelect.max(id)"));
        QueriedSelectRelation outerRelation = (QueriedSelectRelation) relation;
        assertThat(outerRelation.subRelation(), instanceOf(MultiSourceSelect.class));
        assertThat(outerRelation.subRelation().querySpec(),
            isSQL("SELECT max(doc.users.id) GROUP BY doc.users.name"));
    }

    @Test
    public void testSelectDistinctWithGroupByOnSubSelectOuter() {
        QueriedRelation relation = analyze("select distinct max(id) from (" +
                                                   "  select * from users order by name limit 10" +
                                                   ") t group by name order by 1");
        assertThat(relation, instanceOf(QueriedSelectRelation.class));
        assertThat(relation.querySpec(),
            isSQL("SELECT t.max(id) " +
                  "GROUP BY t.max(id) " +
                  "ORDER BY t.max(id)"));
        QueriedSelectRelation outerRelation = (QueriedSelectRelation) relation;
        assertThat(outerRelation.subRelation(), instanceOf(QueriedSelectRelation.class));
        assertThat(outerRelation.subRelation().querySpec(),
            isSQL("SELECT max(doc.users.id) GROUP BY doc.users.name"));
    }

    @Test
    public void testSelectDistinctWithGroupByOnSubSelectInner() {
        QueriedRelation relation =
            analyze("select * from (" +
                    "  select distinct id from users group by id, name order by 1" +
                    ") t order by 1 desc");
        assertThat(relation, instanceOf(QueriedSelectRelation.class));
        QueriedSelectRelation outerRelation = (QueriedSelectRelation) relation;
        assertThat(outerRelation.outputs(), contains(isField("id")));

        assertThat(outerRelation.groupBy(), Matchers.empty());
        assertThat(outerRelation.orderBy().orderBySymbols(), contains(isField("id")));

        assertThat(outerRelation.subRelation(), instanceOf(QueriedSelectRelation.class));
        QueriedSelectRelation subRelation = (QueriedSelectRelation) outerRelation.subRelation();
        assertThat(subRelation.groupBy(), contains(isField("id")));

        assertThat(subRelation.subRelation().groupBy(), contains(isReference("id"), isReference("name")));

    }

    @Test
    public void testGroupByValidationWhenRewritingDistinct() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot use DISTINCT on 'friends': invalid data type 'object_array'");
        analyze("select distinct(friends) from users");
    }

    @Test
    public void testGroupByOnLiteral() throws Exception {
        QueriedRelation relation = analyze(
            "select [1,2,3], count(*) from users u group by 1");
        assertThat(relation.querySpec().outputs(), isSQL("[1, 2, 3], count()"));
        assertThat(relation.querySpec().groupBy(), isSQL("[1, 2, 3]"));
    }

    @Test
    public void testGroupByOnNullLiteral() throws Exception {
        QueriedRelation relation = analyze(
            "select null, count(*) from users u group by 1");
        assertThat(relation.querySpec().outputs(), isSQL("NULL, count()"));
        assertThat(relation.querySpec().groupBy(), isSQL("NULL"));
    }

    @Test
    public void testGroupWithInvalidOrdinal() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("GROUP BY position 2 is not in select list");
        analyze("select name from users u group by 2");
    }

    @Test
    public void testGroupWithInvalidLiteral() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot use 'abc' in GROUP BY clause");
        analyze("select max(id) from users u group by 'abc'");
    }

    @Test
    public void testGroupByOnInvalidNegateLiteral() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("GROUP BY position -4 is not in select list");
        analyze("select count(*), name from sys.nodes group by -4");
    }

    @Test
    public void testGroupWithInvalidNullLiteral() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot use NULL in GROUP BY clause");
        analyze("select max(id) from users u group by NULL");
    }

    @Test
    public void testPositionalArgumentGroupByArrayType() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot GROUP BY 'friends': invalid data type 'object_array'");
        analyze("SELECT sum(id), friends FROM users GROUP BY 2");
    }

    @Test
    public void testGroupByHaving() throws Exception {
        QueriedRelation relation = analyze("select sum(floats) from users group by name having name like 'Slartibart%'");
        assertThat(relation.querySpec().having().query(), isFunction("op_like"));
        Function havingFunction = (Function) relation.querySpec().having().query();
        assertThat(havingFunction.arguments().size(), is(2));
        assertThat(havingFunction.arguments().get(0), isReference("name"));
        assertThat(havingFunction.arguments().get(1), isLiteral("Slartibart%"));
    }

    @Test
    public void testGroupByHavingAliasForRealColumn() {
        QueriedRelation relation = analyze(
            "select id as name from users group by id, name having name != null;");

        HavingClause havingClause = relation.querySpec().having();
        assertThat(havingClause.query(), nullValue());
    }

    @Test
    public void testGroupByHavingNormalize() throws Exception {
        QueriedRelation rel = analyze("select sum(floats) from users group by name having 1 > 4");
        HavingClause having = rel.having();
        assertThat(having.noMatch(), is(true));
        assertNull(having.query());
    }

    @Test
    public void testGroupByHavingOtherColumnInAggregate() throws Exception {
        QueriedRelation relation = analyze("select sum(floats), name from users group by name having max(bytes) = 4");
        assertThat(relation.querySpec().having().query(), isFunction("op_="));
        Function havingFunction = (Function) relation.querySpec().having().query();
        assertThat(havingFunction.arguments().size(), is(2));
        assertThat(havingFunction.arguments().get(0), isFunction("to_long"));
        Function castFunction = (Function) havingFunction.arguments().get(0);
        assertThat(castFunction.arguments().get(0), isFunction("max"));
        Function maxFunction = (Function) castFunction.arguments().get(0);

        assertThat(maxFunction.arguments().get(0), isReference("bytes"));
        assertThat(havingFunction.arguments().get(1), isLiteral(4L));
    }

    @Test
    public void testGroupByHavingOtherColumnOutsideAggregate() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot use column bytes outside of an Aggregation in HAVING clause");
        analyze("select sum(floats) from users group by name having bytes = 4");
    }

    @Test
    public void testGroupByHavingOtherColumnOutsideAggregateInFunction() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot use column bytes outside of an Aggregation in HAVING clause");
        analyze("select sum(floats), name from users group by name having (bytes + 1)  = 4");
    }

    @Test
    public void testGroupByHavingByGroupKey() throws Exception {
        QueriedRelation relation = analyze(
            "select sum(floats), name from users group by name having name like 'Slartibart%'");
        assertThat(relation.querySpec().having().query(), isFunction("op_like"));
        Function havingFunction = (Function) relation.querySpec().having().query();
        assertThat(havingFunction.arguments().size(), is(2));
        assertThat(havingFunction.arguments().get(0), isReference("name"));
        assertThat(havingFunction.arguments().get(1), isLiteral("Slartibart%"));
    }

    @Test
    public void testGroupByHavingComplex() throws Exception {
        QueriedRelation relation = analyze("select sum(floats), name from users " +
                                                   "group by name having 1=0 or sum(bytes) in (42, 43, 44) and  name not like 'Slartibart%'");
        assertThat(relation.querySpec().having().hasQuery(), is(true));
        Function andFunction = (Function) relation.querySpec().having().query();
        assertThat(andFunction, is(notNullValue()));
        assertThat(andFunction.info().ident().name(), is("op_and"));
        assertThat(andFunction.arguments().size(), is(2));

        assertThat(andFunction.arguments().get(0), isFunction("any_="));
        assertThat(andFunction.arguments().get(1), isFunction("op_not"));
    }

    @Test
    public void testGroupByHavingRecursiveFunction() throws Exception {
        QueriedRelation relation = analyze("select sum(floats), name from users " +
                                                   "group by name having sum(power(power(id::double, id::double), id::double)) > 0");
        assertThat(relation.querySpec().having().query(),
            isSQL("(sum(power(power(to_double(doc.users.id), to_double(doc.users.id)), to_double(doc.users.id))) > 0.0)"));
    }

    @Test
    public void testGroupByHiddenColumn() throws Exception {
        expectedException.expect(ColumnUnknownException.class);
        expectedException.expectMessage("Column _docid unknown");
        analyze("select count(*) from users group by _docid");
    }

    @Test
    public void testGroupByGeoShape() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot GROUP BY 'shape': invalid data type 'geo_shape'");
        analyze("select count(*) from users group by shape");
    }

    @Test
    public void testGroupByCastedArray() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot GROUP BY 'to_double_array(loc)': invalid data type 'double_array'");
        analyze("select count(*) from locations group by cast(loc as array(double))");
    }

    @Test
    public void testGroupByCastedArrayByIndex() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot GROUP BY 'to_double_array(loc)': invalid data type 'double_array'");
        analyze("select cast(loc as array(double)) from locations group by 1");
    }
}
