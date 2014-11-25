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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import io.crate.PartitionName;
import io.crate.exceptions.AmbiguousColumnAliasException;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.exceptions.SQLParseException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.MetaDataModule;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.sys.MetaDataSysModule;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.metadata.table.SchemaInfo;
import io.crate.operation.aggregation.impl.AggregationImplModule;
import io.crate.operation.aggregation.impl.AverageAggregation;
import io.crate.operation.aggregation.impl.CollectSetAggregation;
import io.crate.operation.operator.*;
import io.crate.operation.operator.any.AnyEqOperator;
import io.crate.operation.predicate.IsNullPredicate;
import io.crate.operation.predicate.MatchPredicate;
import io.crate.operation.predicate.NotPredicate;
import io.crate.operation.predicate.PredicateModule;
import io.crate.operation.reference.sys.node.NodeLoadExpression;
import io.crate.operation.scalar.CollectionCountFunction;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.operation.scalar.SubscriptFunction;
import io.crate.operation.scalar.arithmetic.AddFunction;
import io.crate.operation.scalar.cast.ToStringArrayFunction;
import io.crate.operation.scalar.cast.ToStringFunction;
import io.crate.operation.scalar.geo.DistanceFunction;
import io.crate.operation.scalar.regex.MatchesFunction;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.*;
import io.crate.testing.MockedClusterServiceModule;
import io.crate.testing.TestingHelpers;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.SetType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.inject.Module;
import org.hamcrest.Matchers;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.*;

import static io.crate.testing.TestingHelpers.*;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class SelectAnalyzerTest extends BaseAnalyzerTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    static class TestMetaDataModule extends MetaDataModule {

        @Override
        protected void bindReferences() {
            super.bindReferences();
            referenceBinder.addBinding(LOAD_INFO.ident()).to(NodeLoadExpression.class).asEagerSingleton();
            referenceBinder.addBinding(CLUSTER_NAME_INFO.ident()).toInstance(new ClusterNameExpression());
        }

        @Override
        protected void bindSchemas() {
            super.bindSchemas();
            SchemaInfo schemaInfo = mock(SchemaInfo.class);
            when(schemaInfo.getTableInfo(TEST_DOC_TABLE_IDENT.name())).thenReturn(userTableInfo);
            when(schemaInfo.getTableInfo(TEST_DOC_TABLE_IDENT_CLUSTERED_BY_ONLY.name())).thenReturn(userTableInfoClusteredByOnly);
            when(schemaInfo.getTableInfo(TEST_DOC_TABLE_IDENT_MULTI_PK.name())).thenReturn(userTableInfoMultiPk);
            when(schemaInfo.getTableInfo(DEEPLY_NESTED_TABLE_IDENT.name())).thenReturn(DEEPLY_NESTED_TABLE_INFO);
            when(schemaInfo.getTableInfo(TEST_PARTITIONED_TABLE_IDENT.name()))
                    .thenReturn(TEST_PARTITIONED_TABLE_INFO);
            when(schemaInfo.getTableInfo(TEST_MULTIPLE_PARTITIONED_TABLE_IDENT.name()))
                    .thenReturn(TEST_MULTIPLE_PARTITIONED_TABLE_INFO);
            when(schemaInfo.getTableInfo(TEST_DOC_TRANSACTIONS_TABLE_IDENT.name()))
                    .thenReturn(TEST_DOC_TRANSACTIONS_TABLE_INFO);
            when(schemaInfo.getTableInfo(TEST_DOC_LOCATIONS_TABLE_IDENT.name()))
                    .thenReturn(TEST_DOC_LOCATIONS_TABLE_INFO);
            when(schemaInfo.getTableInfo(TEST_CLUSTER_BY_STRING_TABLE_INFO.ident().name()))
                    .thenReturn(TEST_CLUSTER_BY_STRING_TABLE_INFO);
            schemaBinder.addBinding(DocSchemaInfo.NAME).toInstance(schemaInfo);
        }

        @Override
        protected void bindFunctions() {
            super.bindFunctions();
            functionBinder.addBinding(YEAR_FUNCTION_INFO.ident()).toInstance(new YearFunction());
        }
    }

    @Override
    protected List<Module> getModules() {
        List<Module> modules = super.getModules();
        modules.addAll(Arrays.<Module>asList(
                new MockedClusterServiceModule(),
                new TestMetaDataModule(),
                new MetaDataSysModule(),
                new OperatorModule(),
                new AggregationImplModule(),
                new PredicateModule(),
                new ScalarFunctionModule()
        ));
        return modules;
    }

    @Override
    protected SelectAnalyzedStatement analyze(String statement) {
        return (SelectAnalyzedStatement) super.analyze(statement);
    }

    @Override
    protected SelectAnalyzedStatement analyze(String statement, Object[] arguments) {
        return (SelectAnalyzedStatement) super.analyze(statement, arguments);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGroupedSelectMissingOutput() throws Exception {
        analyze("select load['5'] from sys.nodes group by load['1']");
    }

    @Test
    public void testIsNullQuery() {
        SelectAnalyzedStatement analysis = analyze("select * from sys.nodes where id is not null");
        assertTrue(analysis.whereClause().hasQuery());
        Function query = (Function)analysis.whereClause().query();

        assertThat(query.info().ident().name(), is(NotPredicate.NAME));
        assertThat(query.arguments().get(0), instanceOf(Function.class));
        Function isNull = (Function) query.arguments().get(0);
        assertThat(isNull.info().ident().name(), is(IsNullPredicate.NAME));
    }

    @Test
    public void testOrderedSelect() throws Exception {
        SelectAnalyzedStatement analysis =  analyze("select load['1'] from sys.nodes order by load['5'] desc");
        assertEquals(analysis.table().ident(), SysNodesTableInfo.IDENT);
        assertNull(analysis.limit());

        assertFalse(analysis.hasGroupBy());
        assertTrue(analysis.orderBy().isSorted());
        assertThat(analysis.table().rowGranularity(), is(RowGranularity.NODE));

        assertEquals(1, analysis.outputSymbols().size());
        assertEquals(1, analysis.orderBy().orderBySymbols().size());
        assertEquals(1, analysis.orderBy().reverseFlags().length);

        assertEquals(LOAD5_INFO, ((Reference) analysis.orderBy().orderBySymbols().get(0)).info());
    }

    @Test
    public void testGroupKeyNotInResultColumnList() throws Exception {
        SelectAnalyzedStatement analysis =  analyze("select count(*) from sys.nodes group by name");

        assertThat(analysis.groupBy().size(), is(1));
        assertThat(analysis.outputNames().get(0), is("count(*)"));
    }

    @Test
    public void testGroupByOnAlias() throws Exception {
        SelectAnalyzedStatement analysis =  analyze("select count(*), name as n from sys.nodes group by n");
        assertThat(analysis.groupBy().size(), is(1));
        assertThat(analysis.outputNames().get(0), is("count(*)"));
        assertThat(analysis.outputNames().get(1), is("n"));

        assertEquals(analysis.groupBy().get(0), analysis.outputSymbols().get(1));
    }

    @Test
    public void testGroupByOnOrdinal() throws Exception {
        // just like in postgres access by ordinal starts with 1
        SelectAnalyzedStatement analysis =  analyze("select count(*), name as n from sys.nodes group by 2");
        assertThat(analysis.groupBy().size(), is(1));
        assertEquals(analysis.groupBy().get(0), analysis.outputSymbols().get(1));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGroupByOnInvalidOrdinal() throws Exception {
        analyze("select count(*), name from sys.nodes group by -4");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGroupByOnOrdinalAggregation() throws Exception {
        analyze("select count(*), name as n from sys.nodes group by 1");
    }

    @Test
    public void testNegativeLiteral() throws Exception {
        SelectAnalyzedStatement analyze = analyze("select * from sys.nodes where port['http'] = -400");
        Function whereClause = (Function)analyze.whereClause().query();
        Symbol symbol = whereClause.arguments().get(1);
        assertThat((Integer) ((Literal) symbol).value(), is(-400));
    }

    @Test
    public void testGroupedSelect() throws Exception {
        SelectAnalyzedStatement analysis =  analyze("select load['1'], count(*) from sys.nodes group by load['1']");
        assertEquals(analysis.table().ident(), SysNodesTableInfo.IDENT);
        assertNull(analysis.limit());

        assertThat(analysis.table().rowGranularity(), is(RowGranularity.NODE));
        assertTrue(analysis.hasGroupBy());
        assertEquals(2, analysis.outputSymbols().size());
        assertEquals(1, analysis.groupBy().size());
        assertEquals(LOAD1_INFO, ((Reference) analysis.groupBy().get(0)).info());

    }


    @Test
    public void testSimpleSelect() throws Exception {
        SelectAnalyzedStatement analysis =  analyze("select load['5'] from sys.nodes limit 2");
        assertEquals(analysis.table().ident(), SysNodesTableInfo.IDENT);
        assertEquals(new Integer(2), analysis.limit());

        assertFalse(analysis.hasGroupBy());

        assertThat(analysis.table().rowGranularity(), is(RowGranularity.NODE));

        assertEquals(SysNodesTableInfo.IDENT, analysis.table().ident());
        assertEquals(1, analysis.outputSymbols().size());
        Reference col1 = (Reference) analysis.outputSymbols().get(0);
        assertEquals(LOAD5_INFO, col1.info());

    }

    @Test
    public void testAggregationSelect() throws Exception {
        SelectAnalyzedStatement analysis =  analyze("select avg(load['5']) from sys.nodes");
        assertEquals(SysNodesTableInfo.IDENT, analysis.table().ident());

        assertThat(analysis.table().rowGranularity(), is(RowGranularity.NODE));

        assertFalse(analysis.hasGroupBy());
        assertEquals(1, analysis.outputSymbols().size());
        Function col1 = (Function) analysis.outputSymbols().get(0);
        assertEquals(FunctionInfo.Type.AGGREGATE, col1.info().type());
        assertEquals(AverageAggregation.NAME, col1.info().ident().name());
    }

    @Test
    public void testAllColumnCluster() throws Exception {
        SelectAnalyzedStatement analysis = analyze("select * from sys.cluster");
        assertThat(analysis.outputNames().size(), is(4));
        assertThat(analysis.outputNames(), containsInAnyOrder("id", "master_node", "name", "settings"));
        assertThat(analysis.outputSymbols().size(), is(4));
    }

    @Test
    public void testAllColumnNodes() throws Exception {
        SelectAnalyzedStatement analysis = analyze("select id, * from sys.nodes");
        assertThat(analysis.outputNames().get(0), is("id"));
        assertThat(analysis.outputNames().get(1), is("id"));
        assertThat(analysis.outputNames().size(), is(14));
        assertEquals(analysis.outputNames().size(), analysis.outputSymbols().size());
    }

    @Test
    public void testWhereSelect() throws Exception {
        SelectAnalyzedStatement analysis =  analyze("select load from sys.nodes " +
                "where load['1'] = 1.2 or 1 >= load['5']");
        assertEquals(SysNodesTableInfo.IDENT, analysis.table().ident());

        assertThat(analysis.table().rowGranularity(), is(RowGranularity.NODE));

        assertFalse(analysis.hasGroupBy());

        Function whereClause = (Function)analysis.whereClause().query();
        assertEquals(OrOperator.NAME, whereClause.info().ident().name());
        assertFalse(whereClause.info().type() == FunctionInfo.Type.AGGREGATE);

        Function left = (Function) whereClause.arguments().get(0);
        assertEquals(EqOperator.NAME, left.info().ident().name());
        assertThat(left.arguments().get(0), IsInstanceOf.instanceOf(Reference.class));
        assertThat(left.arguments().get(1), IsInstanceOf.instanceOf(Literal.class));
        assertSame(((Literal)left.arguments().get(1)).valueType(), DataTypes.DOUBLE);

        Function right = (Function) whereClause.arguments().get(1);
        assertEquals(LteOperator.NAME, right.info().ident().name());
        assertThat(right.arguments().get(0), IsInstanceOf.instanceOf(Reference.class));
        assertThat(right.arguments().get(1), IsInstanceOf.instanceOf(Literal.class));
        assertSame(((Literal) left.arguments().get(1)).valueType(), DataTypes.DOUBLE);
    }

    @Test
    public void testSelectWithParameters() throws Exception {
        SelectAnalyzedStatement analysis = (SelectAnalyzedStatement) analyze("select load from sys.nodes " +
                "where load['1'] = ? or load['5'] <= ? or load['15'] >= ? or load['1'] = ? " +
                "or load['1'] = ? or name = ?", new Object[]{
                1.2d,
                2.4f,
                2L,
                3,
                new Short("1"),
                "node 1"
        });
        Function whereClause = (Function)analysis.whereClause().query();
        assertEquals(OrOperator.NAME, whereClause.info().ident().name());
        assertFalse(whereClause.info().type() == FunctionInfo.Type.AGGREGATE);

        Function function = (Function) whereClause.arguments().get(0);
        assertEquals(OrOperator.NAME, function.info().ident().name());
        function = (Function) function.arguments().get(1);
        assertEquals(EqOperator.NAME, function.info().ident().name());
        assertThat(function.arguments().get(0), IsInstanceOf.instanceOf(Reference.class));
        assertThat(function.arguments().get(1), IsInstanceOf.instanceOf(Literal.class));
        assertEquals(DataTypes.DOUBLE, ((Literal) function.arguments().get(1)).valueType());

        function = (Function) whereClause.arguments().get(1);
        assertEquals(EqOperator.NAME, function.info().ident().name());
        assertThat(function.arguments().get(0), IsInstanceOf.instanceOf(Reference.class));
        assertThat(function.arguments().get(1), IsInstanceOf.instanceOf(Literal.class));
        assertEquals(DataTypes.STRING, ((Literal) function.arguments().get(1)).valueType());
    }

    @Test
    public void testOutputNames() throws Exception {
        SelectAnalyzedStatement analysis = analyze("select load as l, id, load['1'] from sys.nodes");
        assertThat(analysis.outputNames().size(), is(3));
        assertThat(analysis.outputNames().get(0), is("l"));
        assertThat(analysis.outputNames().get(1), is("id"));
        assertThat(analysis.outputNames().get(2), is("load['1']"));
    }

    @Test
    public void testDuplicateOutputNames() throws Exception {
        SelectAnalyzedStatement analysis = analyze("select load as l, load['1'] as l from sys.nodes");
        assertThat(analysis.outputNames().size(), is(2));
        assertThat(analysis.outputNames().get(0), is("l"));
        assertThat(analysis.outputNames().get(1), is("l"));
    }

    @Test
    public void testOrderByOnAlias() throws Exception {
        SelectAnalyzedStatement analyze =  analyze(
                "select name as cluster_name from sys.cluster order by cluster_name");
        assertThat(analyze.outputNames().size(), is(1));
        assertThat(analyze.outputNames().get(0), is("cluster_name"));

        assertTrue(analyze.orderBy().isSorted());
        assertThat(analyze.orderBy().orderBySymbols().size(), is(1));
        assertThat(analyze.orderBy().orderBySymbols().get(0), is(analyze.outputSymbols().get(0)));
    }

    @Test(expected = AmbiguousColumnAliasException.class)
    public void testAmbiguousOrderByOnAlias() throws Exception {
        analyze("select id as load, load from sys.nodes order by load");
    }

    @Test
    public void testOffsetSupportInAnalyzer() throws Exception {
        SelectAnalyzedStatement analyze =  analyze("select * from sys.nodes limit 1 offset 3");
        assertThat(analyze.offset(), is(3));
    }

    @Test
    public void testNoMatchStatement() throws Exception {
        for (String stmt : ImmutableList.of(
                "select id from sys.nodes where false",
                "select id from sys.nodes where 1=0",
                "select id from sys.nodes where sys.cluster.name = 'something'"
        )) {
            SelectAnalyzedStatement analysis = analyze(stmt);
            assertTrue(stmt, analysis.noMatch());
            assertFalse(stmt, analysis.whereClause().hasQuery());
        }
    }

    @Test
    public void testEvaluatingMatchAllStatement() throws Exception {
        SelectAnalyzedStatement analysis = analyze("select id from sys.nodes where sys.cluster.name = 'testcluster'");
        assertFalse(analysis.noMatch());
        assertFalse(analysis.whereClause().hasQuery());
    }

    @Test
    public void testAllMatchStatement() throws Exception {
        for (String stmt : ImmutableList.of(
                "select id from sys.nodes where true",
                "select id from sys.nodes where 1=1",
                "select id from sys.nodes"
        )) {
            System.out.println(stmt);
            SelectAnalyzedStatement analysis = analyze(stmt);
            assertFalse(stmt, analysis.noMatch());
            assertFalse(stmt, analysis.whereClause().hasQuery());
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
            SelectAnalyzedStatement analysis = analyze(statement);
            WhereClause whereClause = analysis.whereClause();

            Function notFunction = (Function)whereClause.query();
            assertThat(notFunction.info().ident().name(), is(NotPredicate.NAME));
            assertThat(notFunction.arguments().size(), is(1));

            Function eqFunction = (Function) notFunction.arguments().get(0);
            assertThat(eqFunction.info().ident().name(), is(EqOperator.NAME));
            assertThat(eqFunction.arguments().size(), is(2));

            List<Symbol> eqArguments = eqFunction.arguments();
            assertThat(eqArguments.get(0), instanceOf(Reference.class));
            assertLiteralSymbol(eqArguments.get(1), "something");
        }
    }

    @Test
    public void testRewriteRegexpNoMatch() throws Exception {
        String statement = "select * from sys.nodes where sys.nodes.name !~ '[sS]omething'";
        SelectAnalyzedStatement analysis =  analyze(statement);
        WhereClause whereClause = analysis.whereClause();

        Function notFunction = (Function) whereClause.query();
        assertThat(notFunction.info().ident().name(), is(NotPredicate.NAME));
        assertThat(notFunction.arguments().size(), is(1));

        Function eqFunction = (Function) notFunction.arguments().get(0);
        assertThat(eqFunction.info().ident().name(), is(RegexpMatchOperator.NAME));
        assertThat(eqFunction.arguments().size(), is(2));

        List<Symbol> eqArguments = eqFunction.arguments();
        assertThat(eqArguments.get(0), instanceOf(Reference.class));
        assertLiteralSymbol(eqArguments.get(1), "[sS]omething");

    }

    @Test
    public void testClusteredBy() throws Exception {
        SelectAnalyzedStatement analysis = analyze("select name from users where id=1");
        assertEquals(ImmutableList.of("1"), analysis.routingValues());
        assertEquals("1", analysis.whereClause().clusteredBy().get());

        analysis = analyze("select name from users where id=1 or id=2");
        assertEquals(ImmutableList.of("1", "2"), analysis.routingValues());
        assertFalse(analysis.whereClause().clusteredBy().isPresent());
    }

    @Test
    public void testClusteredByOnly() throws Exception {
        SelectAnalyzedStatement analysis = analyze("select name from users_clustered_by_only where id=1");
        assertEquals(ImmutableList.of(), analysis.ids());
        assertEquals(ImmutableList.of("1"), analysis.routingValues());
        assertEquals("1", analysis.whereClause().clusteredBy().get());

        analysis = analyze("select name from users_clustered_by_only where id=1 or id=2");
        assertEquals(ImmutableList.of(), analysis.ids());
        assertEquals(ImmutableList.of(), analysis.routingValues());
        assertFalse(analysis.whereClause().clusteredBy().isPresent());

        analysis = analyze("select name from users_clustered_by_only where id=1 and id=2");
        assertEquals(ImmutableList.of(), analysis.ids());
        assertEquals(ImmutableList.of(), analysis.routingValues());
        assertFalse(analysis.whereClause().clusteredBy().isPresent());
    }

    @Test
    public void testCompositePrimaryKey() throws Exception {
        SelectAnalyzedStatement analysis = analyze("select name from users_multi_pk where id=1");
        assertEquals(ImmutableList.of(), analysis.ids());
        assertEquals(ImmutableList.of(), analysis.routingValues());
        assertEquals("1", analysis.whereClause().clusteredBy().get());

        analysis = analyze("select name from users_multi_pk where id=1 and name='Douglas'");
        assertEquals(ImmutableList.of("AgExB0RvdWdsYXM="), analysis.ids());
        assertEquals(ImmutableList.of("1"), analysis.routingValues());
        assertEquals("1", analysis.whereClause().clusteredBy().get());

        analysis = analyze("select name from users_multi_pk where id=1 or id=2 and name='Douglas'");
        assertEquals(ImmutableList.of(), analysis.ids());
        assertEquals(ImmutableList.of(), analysis.routingValues());
        assertFalse(analysis.whereClause().clusteredBy().isPresent());

        analysis = analyze("select name from users_multi_pk where id=1 and name='Douglas' or name='Arthur'");
        assertEquals(ImmutableList.of(), analysis.ids());
        assertEquals(ImmutableList.of(), analysis.routingValues());
        assertFalse(analysis.whereClause().clusteredBy().isPresent());
    }

    @Test
    public void testPrimaryKeyAndVersion() throws Exception {
        SelectAnalyzedStatement analysis = analyze(
            "select name from users where id = 2 and \"_version\" = 1");
        assertEquals(ImmutableList.of("2"), analysis.ids());
        assertEquals(ImmutableList.of("2"), analysis.routingValues());
        assertThat(analysis.whereClause().version().get(), is(1L));
    }

    @Test
    public void testMultiplePrimaryKeys() throws Exception {
        SelectAnalyzedStatement analysis = analyze(
            "select name from users where id = 2 or id = 1");

        assertEquals(ImmutableList.of("1", "2"), analysis.ids());
        assertEquals(ImmutableList.of("1", "2"), analysis.routingValues());
    }

    @Test
    public void testMultiplePrimaryKeysAndInvalidColumn() throws Exception {
        SelectAnalyzedStatement analysis = analyze(
            "select name from users where id = 2 or id = 1 and name = 'foo'");
        assertEquals(0, analysis.ids().size());
    }

    @Test
    public void testNotEqualsDoesntMatchPrimaryKey() throws Exception {
        SelectAnalyzedStatement analysis = analyze("select name from users where id != 1");
        assertEquals(0, analysis.ids().size());
        assertEquals(0, analysis.routingValues().size());
    }

    @Test
    public void testMultipleCompoundPrimaryKeys() throws Exception {
        SelectAnalyzedStatement analysis = analyze(
            "select * from sys.shards where (schema_name='doc' and id = 1 and table_name = 'foo' and partition_ident='') " +
                    "or (schema_name='doc' and id = 2 and table_name = 'bla' and partition_ident='')");
        assertEquals(ImmutableList.of("BANkb2MDZm9vATEA", "BANkb2MDYmxhATIA"), analysis.ids());
        assertEquals(ImmutableList.of("BANkb2MDZm9vATEA", "BANkb2MDYmxhATIA"), analysis.routingValues());
        assertFalse(analysis.whereClause().clusteredBy().isPresent());

        analysis = analyze(
            "select * from sys.shards where (schema_name='doc' and id = 1 and table_name = 'foo') " +
                    "or (schema_name='doc' and id = 2 and table_name = 'bla') or id = 1");
        assertEquals(ImmutableList.of(), analysis.ids());
        assertEquals(ImmutableList.of(), analysis.routingValues());
        assertFalse(analysis.whereClause().clusteredBy().isPresent());
    }

    @Test
    public void test1ColPrimaryKey() throws Exception {
        SelectAnalyzedStatement analysis = analyze("select name from sys.nodes where id='jalla'");
        assertEquals(ImmutableList.of("jalla"), analysis.ids());
        assertEquals(ImmutableList.of("jalla"), analysis.routingValues());

        analysis = analyze("select name from sys.nodes where 'jalla'=id");
        assertEquals(ImmutableList.of("jalla"), analysis.ids());

        analysis = analyze("select name from sys.nodes where id='jalla' and id='jalla'");
        assertEquals(ImmutableList.of("jalla"), analysis.ids());

        analysis = analyze("select name from sys.nodes where id='jalla' and (id='jalla' or 1=1)");
        assertEquals(ImmutableList.of("jalla"), analysis.ids());

        // a no match results in undefined key literals, since those are ambiguous
        analysis = analyze("select name from sys.nodes where id='jalla' and id='kelle'");
        assertEquals(ImmutableList.of(), analysis.ids());
        assertTrue(analysis.noMatch());

        analysis = analyze("select name from sys.nodes where id='jalla' or name = 'something'");
        assertEquals(ImmutableList.of(), analysis.ids());
        assertFalse(analysis.noMatch());

        analysis = analyze("select name from sys.nodes where name = 'something'");
        assertEquals(ImmutableList.of(), analysis.ids());
        assertFalse(analysis.noMatch());

    }

    @Test
    public void test3ColPrimaryKey() throws Exception {
        SelectAnalyzedStatement analysis = analyze("select id from sys.shards where id=1 and table_name='jalla' and schema_name='doc' and partition_ident=''");
        // base64 encoded versions of Streamable of ["doc","jalla","1"]
        assertEquals(ImmutableList.of("BANkb2MFamFsbGEBMQA="), analysis.ids());
        assertEquals(ImmutableList.of("BANkb2MFamFsbGEBMQA="), analysis.routingValues());
        assertFalse(analysis.noMatch());

        analysis = analyze("select id from sys.shards where id=1 and table_name='jalla' and id=1 and schema_name='doc' and partition_ident=''");
        // base64 encoded versions of Streamable of ["doc","jalla","1"]
        assertEquals(ImmutableList.of("BANkb2MFamFsbGEBMQA="), analysis.ids());
        assertEquals(ImmutableList.of("BANkb2MFamFsbGEBMQA="), analysis.routingValues());
        assertFalse(analysis.noMatch());


        analysis = analyze("select id from sys.shards where id=1");
        assertEquals(ImmutableList.of(), analysis.ids());
        assertFalse(analysis.noMatch());

        analysis = analyze("select id from sys.shards where id=1 and schema_name='doc' and table_name='jalla' and id=2 and partition_ident=''");
        assertEquals(ImmutableList.of(), analysis.ids());
        assertTrue(analysis.noMatch());
    }

    @Test
    public void test1ColPrimaryKeySetLiteralDiffMatches() throws Exception {
        SelectAnalyzedStatement analysis = analyze(
                "select name from sys.nodes where id in ('jalla', 'kelle') and id in ('jalla', 'something')");
        assertFalse(analysis.noMatch());
        assertEquals(1, analysis.ids().size());
        assertEquals("jalla", analysis.ids().get(0));
    }


    @Test
    public void test1ColPrimaryKeySetLiteral() throws Exception {
        SelectAnalyzedStatement analysis = analyze("select name from sys.nodes where id in ('jalla', 'kelle')");
        assertFalse(analysis.noMatch());
        assertEquals(2, analysis.ids().size());
        assertThat(analysis.ids(), Matchers.containsInAnyOrder("jalla", "kelle"));
    }

    @Test
    public void test3ColPrimaryKeySetLiteral() throws Exception {
        SelectAnalyzedStatement analysis = analyze("select id from sys.shards where id=1 and schema_name='doc' and table_name in ('jalla', 'kelle') and partition_ident=''");
        assertEquals(2, analysis.ids().size());
        // base64 encoded versions of Streamable of ["doc","jalla","1"] and ["doc","kelle","1"]

        assertThat(analysis.ids(), Matchers.containsInAnyOrder("BANkb2MFamFsbGEBMQA=", "BANkb2MFa2VsbGUBMQA="));
        assertThat(analysis.routingValues(), Matchers.containsInAnyOrder("BANkb2MFamFsbGEBMQA=", "BANkb2MFa2VsbGUBMQA="));
    }

    @Test
    public void testGranularityWithSingleAggregation() throws Exception {
        SelectAnalyzedStatement analysis = analyze("select count(*) from sys.nodes");
        assertThat(analysis.table().rowGranularity(), is(RowGranularity.NODE));
    }

    @Test
    public void testRewriteCountStringLiteral() {
        SelectAnalyzedStatement analysis = analyze("select count('id') from sys.nodes");
        List<Symbol> outputSymbols = analysis.outputSymbols;
        assertThat(outputSymbols.size(), is(1));
        assertThat(outputSymbols.get(0), instanceOf(Function.class));
        assertThat(((Function) outputSymbols.get(0)).arguments().size(), is(0));
    }

    @Test
    public void testRewriteCountNull() {
        SelectAnalyzedStatement analysis = analyze("select count(null) from sys.nodes");
        List<Symbol> outputSymbols = analysis.outputSymbols;
        assertThat(outputSymbols.size(), is(1));
        assertThat(outputSymbols.get(0), instanceOf(Literal.class));
        assertThat((Long) ((Literal) outputSymbols.get(0)).value(), is(0L));
    }

    @Test
    public void testWhereInSelect() throws Exception {
        SelectAnalyzedStatement analysis = analyze("select load from sys.nodes where load['1'] in (1.0, 2.0, 4.0, 8.0, 16.0)");

        Function whereClause = (Function)analysis.whereClause().query();
        assertEquals(InOperator.NAME, whereClause.info().ident().name());
        assertThat(whereClause.arguments().get(0), IsInstanceOf.instanceOf(Reference.class));
        assertThat(whereClause.arguments().get(1), IsInstanceOf.instanceOf(Literal.class));

        Literal setLiteral = (Literal)whereClause.arguments().get(1);
        assertEquals(new SetType(DataTypes.DOUBLE), setLiteral.valueType());
    }

    @Test
    public void testWhereInSelectDifferentDataTypeList() throws Exception {
        SelectAnalyzedStatement analysis = analyze("select 'found' from users where 1 in (1.2, 2)");
        assertFalse(analysis.whereClause().hasQuery()); // already normalized from 1 in (1, 2) --> true
        assertFalse(analysis.whereClause().noMatch());
    }

    @Test
    public void testWhereInSelectDifferentDataTypeValue() throws Exception {
        SelectAnalyzedStatement analysis = analyze("select 'found' from users where 1.2 in (1, 2)");
        assertFalse(analysis.whereClause().hasQuery()); // already normalized from 1.2 in (1.0, 2.0) --> false
        assertTrue(analysis.noMatch());
        assertTrue(analysis.hasNoResult());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWhereInSelectDifferentDataTypeValueUncompatibleDataTypes() throws Exception {
        analyze("select 'found' from users where 1 in (1, 'foo', 2)");
    }

    @Test
    public void testAggregationDistinct() {
        SelectAnalyzedStatement analysis =  analyze("select count(distinct load['1']) from sys.nodes");

        assertTrue(analysis.hasAggregates());
        assertEquals(2, analysis.functions().size());

        Function collectionCount = getFunctionByName(CollectionCountFunction.NAME, analysis.functions());
        Function collectSet = getFunctionByName(CollectSetAggregation.NAME, analysis.functions());
        assertNotNull(collectionCount);
        assertNotNull(collectSet);

        List<Symbol> args = collectionCount.arguments();
        assertEquals(1, args.size());
        Function innerFunction = (Function) args.get(0);
        assertTrue(innerFunction.info().type() == FunctionInfo.Type.AGGREGATE);
        assertEquals(innerFunction.info().ident().name(), CollectSetAggregation.NAME);
        List<Symbol> innerArguments = innerFunction.arguments();
        assertThat(innerArguments.get(0), IsInstanceOf.instanceOf(Reference.class));
        assertThat(((Reference) innerArguments.get(0)).info(), IsInstanceOf.instanceOf(ReferenceInfo.class));
        ReferenceInfo refInfo = ((Reference) innerArguments.get(0)).info();
        assertThat(refInfo.ident().columnIdent().name(), is("load"));
        assertThat(refInfo.ident().columnIdent().path().get(0), is("1"));

        assertSame(collectSet, innerFunction);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSelectAggregationMissingGroupBy() {
        analyze("select name, count(id) from users");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSelectGlobalDistinctAggregationMissingGroupBy() {
        analyze("select distinct name, count(id) from users");
    }

    @Test
    public void testSelectGlobalDistinctAggregate() {
        SelectAnalyzedStatement distinctAnalysis =  analyze("select distinct count(*) from users");
        assertFalse(distinctAnalysis.hasGroupBy());
    }

    @Test
    public void testSelectGlobalDistinctRewriteAggregateionGroupBy() {
        SelectAnalyzedStatement distinctAnalysis =  analyze("select distinct name, count(id) from users group by name");
        SelectAnalyzedStatement groupByAnalysis =  analyze("select name, count(id) from users group by name");
        assertEquals(groupByAnalysis.groupBy(), distinctAnalysis.groupBy());
    }

    @Test
    public void testSelectGlobalDistinctRewrite() {
        SelectAnalyzedStatement distinctAnalysis =  analyze("select distinct name from users");
        SelectAnalyzedStatement groupByAnalysis =  analyze("select name from users group by name");
        assertEquals(groupByAnalysis.groupBy(), distinctAnalysis.groupBy());
    }

    @Test
    public void testSelectGlobalDistinctRewriteAllColumns() {
        SelectAnalyzedStatement distinctAnalysis =  analyze("select distinct * from transactions");
        SelectAnalyzedStatement groupByAnalysis =
                 analyze(
                        "select id, sender, recipient, amount, timestamp " +
                        "from transactions " +
                        "group by id, sender, recipient, amount, timestamp");
        assertEquals(groupByAnalysis.groupBy().size(), distinctAnalysis.groupBy().size());
        for (Symbol s : distinctAnalysis.groupBy()) {
            assertTrue(distinctAnalysis.groupBy().contains(s));
        }
    }

    @Test
    public void testSelectWithObjectLiteral() throws Exception {
        Map<String, Object> map = new HashMap<>();
        map.put("1", 1.0);
        map.put("5", 2.5);
        map.put("15", 8.0);
        SelectAnalyzedStatement analysis =  (SelectAnalyzedStatement) analyze("select id from sys.nodes where load=?",
                new Object[]{map});
        Function whereClause = (Function)analysis.whereClause().query();
        assertThat(whereClause.arguments().get(1), instanceOf(Literal.class));
        assertTrue(((Object) ((Literal) whereClause.arguments().get(1)).value()).equals(map));
    }

    @Test
    public void testLikeInWhereQuery() {
        SelectAnalyzedStatement analysis = analyze("select * from sys.nodes where name like 'foo'");

        assertNotNull(analysis.whereClause());
        Function whereClause = (Function)analysis.whereClause().query();
        assertEquals(LikeOperator.NAME, whereClause.info().ident().name());
        ImmutableList<DataType> argumentTypes = ImmutableList.<DataType>of(DataTypes.STRING, DataTypes.STRING);
        assertEquals(argumentTypes, whereClause.info().ident().argumentTypes());

        assertThat(whereClause.arguments().get(0), IsInstanceOf.instanceOf(Reference.class));
        assertThat(whereClause.arguments().get(1), IsInstanceOf.instanceOf(Literal.class));
        Literal stringLiteral = (Literal) whereClause.arguments().get(1);
        assertThat((BytesRef) stringLiteral.value(), is(new BytesRef(("foo"))));
    }

    @Test(expected = UnsupportedOperationException.class) // ESCAPE is not supported yet.
    public void testLikeEscapeInWhereQuery() {
        analyze("select * from sys.nodes where name like 'foo' escape 'o'");
    }

    @Test
    public void testLikeNoStringDataTypeInWhereQuery() {
        SelectAnalyzedStatement analysis = analyze("select * from sys.nodes where name like 1");

        // check if the implicit cast of the pattern worked
        ImmutableList<DataType> argumentTypes = ImmutableList.<DataType>of(DataTypes.STRING, DataTypes.STRING);
        Function whereClause = (Function)analysis.whereClause().query();
        assertEquals(argumentTypes, whereClause.info().ident().argumentTypes());
        assertThat(whereClause.arguments().get(1), IsInstanceOf.instanceOf(Literal.class));
        Literal stringLiteral = (Literal) whereClause.arguments().get(1);
        assertThat((BytesRef) stringLiteral.value(), is(new BytesRef("1")));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testLikeReferenceInPatternInWhereQuery() {
        analyze("select * from sys.nodes where 1 like name");
    }

    @Test
    public void testLikeLongDataTypeInWhereQuery() {
        SelectAnalyzedStatement analysis = analyze("select * from sys.nodes where 1 like 2");

        // check if implicit cast worked of both, expression and pattern.
        Function function = (Function) analysis.functions().toArray()[0];
        assertEquals(LikeOperator.NAME, function.info().ident().name());
        ImmutableList<DataType> argumentTypes = ImmutableList.<DataType>of(DataTypes.STRING, DataTypes.STRING);
        assertEquals(argumentTypes, function.info().ident().argumentTypes());

        assertThat(function.arguments().get(0), IsInstanceOf.instanceOf(Literal.class));
        assertThat(function.arguments().get(1), IsInstanceOf.instanceOf(Literal.class));
        Literal expressionLiteral = (Literal) function.arguments().get(0);
        Literal patternLiteral = (Literal) function.arguments().get(1);
        assertThat((BytesRef)expressionLiteral.value(), is(new BytesRef("1")));
        assertThat((BytesRef) patternLiteral.value(), is(new BytesRef("2")));
    }

    @Test
    public void testIsNullInWhereQuery() {
        SelectAnalyzedStatement analysis = analyze("select * from sys.nodes where name is null");
        Function isNullFunction = (Function) analysis.functions().toArray()[0];

        assertThat(isNullFunction.info().ident().name(), is(IsNullPredicate.NAME));
        assertThat(isNullFunction.arguments().size(), is(1));
        assertThat(isNullFunction.arguments().get(0), IsInstanceOf.instanceOf(Reference.class));
        assertNotNull(analysis.whereClause());
    }

    @Test
    public void testNullIsNullInWhereQuery() {
        SelectAnalyzedStatement analysis = analyze("select * from sys.nodes where null is null");
        Function isNullFunction = (Function) analysis.functions().toArray()[0];
        assertThat(isNullFunction.arguments().get(0), IsInstanceOf.instanceOf(Literal.class));
        assertFalse(analysis.whereClause().hasQuery());
        assertFalse(analysis.noMatch());
    }

    @Test
    public void testLongIsNullInWhereQuery() {
        SelectAnalyzedStatement analysis = analyze("select * from sys.nodes where 1 is null");
        Function isNullFunction = (Function) analysis.functions().toArray()[0];
        assertThat(isNullFunction.arguments().get(0), IsInstanceOf.instanceOf(Literal.class));
        assertTrue(analysis.noMatch());
    }

    @Test
    public void testIsNullOnDynamicReference() {
        SelectAnalyzedStatement analysis = analyze("select \"_id\" from users where details['invalid'] is null");
        assertTrue(analysis.noMatch());
    }

    @Test
    public void testNotPredicate() {
        SelectAnalyzedStatement analysis = analyze("select * from users where name not like 'foo%'");
        assertThat(((Function)analysis.whereClause.query()).info().ident().name(), is(NotPredicate.NAME));
    }

    @Test
    public void testFilterByLiteralBoolean() throws Exception {
        SelectAnalyzedStatement analysis = analyze("select * from users where awesome=TRUE");
        assertThat(((Function) analysis.whereClause().query()).arguments().get(1).symbolType(), is(SymbolType.LITERAL));
    }

    @Test(expected = SQLParseException.class)
    public void testNoFrom() throws Exception {
        analyze("select name");
    }

    @Test(expected = SQLParseException.class)
    public void test2From() throws Exception {
        analyze("select name from a, b");
    }


    @Test
    public void testHasNoResult() {
        AnalyzedStatement analyzedStatement = analyze("select count(*) from users limit 1 offset 1");
        assertTrue(analyzedStatement.hasNoResult());

        analyzedStatement = analyze("select count(*) from users limit 5 offset 1");
        assertTrue(analyzedStatement.hasNoResult());

        analyzedStatement = analyze("select count(*) from users limit 1");
        assertFalse(analyzedStatement.hasNoResult());

        analyzedStatement = analyze("select count(*) from users limit 0");
        assertTrue(analyzedStatement.hasNoResult());

        analyzedStatement = analyze("select name from users limit 0");
        assertTrue(analyzedStatement.hasNoResult());

        analyzedStatement = analyze("select name from users where false");
        assertTrue(analyzedStatement.hasNoResult());

        analyzedStatement = analyze("select name from users limit 10 offset 10");
        assertFalse(analyzedStatement.hasNoResult());
    }

    @Test(expected = UnsupportedFeatureException.class)
    public void testQueryRequiresScalar() throws Exception {
        // only scalar functions are allowed on system tables because we have no lucene queries
        analyze("select * from sys.shards where match(table_name, 'characters')");
    }

    @Test (expected = IllegalArgumentException.class)
    public void testLimitWithWrongArgument() throws Exception {
        analyze("select * from sys.shards limit ?", new Object[] { "invalid" });
    }

    @Test (expected = UnsupportedOperationException.class)
    public void testSelectSysExpressionWithoutGroupBy() throws Exception {
        analyze("select sys.cluster.name, id from users");
    }

    @Test (expected = UnsupportedOperationException.class)
    public void testWhereSysExpressionWithoutGroupBy() throws Exception {
        analyze("select id from users where sys.cluster.name ='crate'");
    }

    @Test (expected = UnsupportedOperationException.class)
    public void testSelectFunctionWithSysExpressionWithoutGroupBy() throws Exception {
        analyze("select format('%s', sys.nodes.id), id from users");
    }

    @Test (expected = UnsupportedOperationException.class)
    public void testWhereFunctionWithSysExpressionWithoutGroupBy() throws Exception {
        analyze("select id from users where format('%s', sys.nodes.id) = 'foo'");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testOrderByQualifiedName() throws Exception {
        // caused by:
        // select * from tweets order by user.id
        // table tweets has fields "id" and user['id']
        // this order by clause referenced id, not user['id']
        analyze("select * from users order by friends.id");
    }

    @Test
    public void testNotTimestamp() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid argument of type \"timestamp\" passed to (NOT \"date\") predicate. " +
            "Argument must resolve to boolean or null");

        analyze("select id, name from parted where not date");
    }

    @Test
    public void testSelectFromPartitionedTable() throws Exception {
        String partition1 = new PartitionName("parted", Arrays.asList(new BytesRef("1395874800000"))).stringValue();
        String partition2 = new PartitionName("parted", Arrays.asList(new BytesRef("1395961200000"))).stringValue();
        String partition3 = new PartitionName("parted", new ArrayList<BytesRef>(){{add(null);}}).stringValue();

        SelectAnalyzedStatement analysis = analyze("select id, name from parted where date = 1395874800000");
        assertEquals(ImmutableList.of(partition1), analysis.whereClause().partitions());
        assertFalse(analysis.whereClause().hasQuery());
        assertFalse(analysis.noMatch());

        analysis = analyze("select id, name from parted where date = 1395874800000 " +
            "and substr(name, 0, 4) = 'this'");
        assertEquals(ImmutableList.of(partition1), analysis.whereClause().partitions());
        assertThat(analysis.whereClause().hasQuery(), is(true));
        assertThat(analysis.noMatch(), is(false));

        analysis = analyze("select id, name from parted where date >= 1395874800000");
        assertThat(analysis.whereClause().partitions(), containsInAnyOrder(partition1, partition2));
        assertFalse(analysis.whereClause().hasQuery());
        assertFalse(analysis.noMatch());

        analysis = analyze("select id, name from parted where date < 1395874800000");
        assertEquals(ImmutableList.of(), analysis.whereClause().partitions());
        assertTrue(analysis.noMatch());

        analysis = analyze("select id, name from parted where date = 1395874800000 and date = 1395961200000");
        assertEquals(ImmutableList.of(), analysis.whereClause().partitions());
        assertTrue(analysis.noMatch());

        analysis = analyze("select id, name from parted where date = 1395874800000 or date = 1395961200000");
        assertThat(analysis.whereClause().partitions(), containsInAnyOrder(partition1, partition2));
        assertFalse(analysis.whereClause().hasQuery());
        assertFalse(analysis.noMatch());

        analysis = analyze("select id, name from parted where date < 1395874800000 or date > 1395874800000");
        assertEquals(ImmutableList.of(partition2), analysis.whereClause().partitions());
        assertFalse(analysis.whereClause().hasQuery());
        assertFalse(analysis.noMatch());

        analysis = analyze("select id, name from parted where date in (1395874800000, 1395961200000)");
        assertThat(analysis.whereClause().partitions(), containsInAnyOrder(partition1, partition2));
        assertFalse(analysis.whereClause().hasQuery());
        assertFalse(analysis.noMatch());

        analysis = analyze("select id, name from parted where date in (1395874800000, 1395961200000) and id = 1");
        assertThat(analysis.whereClause().partitions(), containsInAnyOrder(partition1, partition2));
        assertTrue(analysis.whereClause().hasQuery());
        assertFalse(analysis.noMatch());

        /**
         * col = 'undefined' -> null as col doesn't exist
         * ->
         *  not (true  and null) -> not (null)  -> no match
         *  not (null  and null) -> not (null)  -> no match
         *  not (false and null) -> not (false) -> match
         */
        analysis = analyze("select id, name from parted where not (date = 1395874800000 and obj['col'] = 'undefined')");
        assertThat(analysis.whereClause().partitions(), containsInAnyOrder(partition2));
        assertThat(analysis.whereClause().hasQuery(), is(false));
        assertThat(analysis.noMatch(), is(false));

        analysis = analyze("select id, name from parted where date in (1395874800000) or date in (1395961200000)");
        assertThat(analysis.whereClause().partitions(), containsInAnyOrder(partition1, partition2));
        assertFalse(analysis.whereClause().hasQuery());
        assertFalse(analysis.noMatch());

        analysis = analyze("select id, name from parted where date = 1395961200000 and id = 1");
        assertEquals(ImmutableList.of(partition2), analysis.whereClause().partitions());
        assertTrue(analysis.whereClause().hasQuery());
        assertFalse(analysis.noMatch());

        analysis = analyze("select id, name from parted where (date =1395874800000 or date = 1395961200000) and id = 1");
        assertThat(analysis.whereClause().partitions(), containsInAnyOrder(partition1, partition2));
        assertTrue(analysis.whereClause().hasQuery());
        assertFalse(analysis.noMatch());

        analysis = analyze("select id, name from parted where date = 1395874800000 and id is null");
        assertEquals(ImmutableList.of(partition1), analysis.whereClause().partitions());
        assertTrue(analysis.whereClause().hasQuery());
        assertFalse(analysis.noMatch());

        analysis = analyze("select id, name from parted where date is null and id = 1");
        assertEquals(ImmutableList.of(partition3), analysis.whereClause().partitions());
        assertTrue(analysis.whereClause().hasQuery());
        assertFalse(analysis.noMatch());

        analysis = analyze("select id, name from parted where 1395874700000 < date and date < 1395961200001");
        assertThat(analysis.whereClause().partitions(), containsInAnyOrder(partition1, partition2));
        assertFalse(analysis.whereClause().hasQuery());
        assertFalse(analysis.noMatch());

        analysis = analyze("select id, name from parted where '2014-03-16T22:58:20' < date and date < '2014-03-27T23:00:01'");
        assertThat(analysis.whereClause().partitions(), containsInAnyOrder(partition1, partition2));
        assertFalse(analysis.whereClause().hasQuery());
        assertFalse(analysis.noMatch());
    }

    @Test
    public void testSelectFromPartitionedTableUnsupported() throws Exception {
        // these queries won't work because we would have to execute 2 separate ESSearch tasks
        // and merge results which is not supported right now and maybe never will be
        try {
            analyze("select id, name from parted where date = 1395961200000 or id = 1");
            fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            assertThat(e.getMessage(),
                is("logical conjunction of the conditions in the WHERE clause which involve " +
                    "partitioned columns led to a query that can't be executed."));
        }

        try {
            analyze("select id, name from parted where id = 1 or date = 1395961200000");
            fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            assertThat(e.getMessage(),
                is("logical conjunction of the conditions in the WHERE clause which involve " +
                    "partitioned columns led to a query that can't be executed."));
        }
    }

    @Test
    public void testSelectPartitionedTableOrderBy() throws Exception {
        SelectAnalyzedStatement analysis = analyze(
                "select id from multi_parted order by id, abs(num)");
        List<Symbol> symbols = analysis.orderBy().orderBySymbols();
        assert symbols != null;
        assertThat(symbols.size(), is(2));
        assertThat(symbols.get(0), Matchers.instanceOf(Reference.class));
        assertThat(symbols.get(1), Matchers.instanceOf(Function.class));
    }

    @Test
    public void testSortOnUnknownColumn() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Cannot order by \"users.o['unknown_column']\". The column doesn't exist.");
        analyze("select name from users order by o['unknown_column']");

    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSelectPartitionedTableOrderByPartitionedColumn() throws Exception {
        analyze("select name from parted order by date");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSelectPartitionedTableOrderByPartitionedColumnInFunction() throws Exception {
        analyze("select name from parted order by year(date)");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSelectOrderByPartitionedNestedColumn() throws Exception {
        analyze("select id from multi_parted order by obj['name']");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSelectOrderByPartitionedNestedColumnInFunction() throws Exception {
        analyze("select id from multi_parted order by format('abc %s', obj['name'])");
    }


    @Test(expected = UnsupportedOperationException.class)
    public void testJoin() throws Exception {
        analyze("select * from users join users_multi_pk on users.id = users.multi_pk.id");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testUnion() throws Exception {
        analyze("select * from users union select * from users_multi_pk");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testArrayCompareInvalidArray() throws Exception {
        analyze("select * from users where 'George' = ANY (name)");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testArrayCompareObjectArray() throws Exception {
        // TODO: remove this artificial limitation in general
        analyze("select * from users where ? = ANY (friends)", new Object[]{
                new MapBuilder<String, Object>().put("id", 1L).map()
        });
    }

    @Test
    public void testArrayCompareAny() throws Exception {
        SelectAnalyzedStatement analysis =  analyze("select * from users where 0 = ANY (counters)");
        assertThat(analysis.whereClause().hasQuery(), is(true));

        FunctionInfo anyInfo = ((Function)analysis.whereClause().query()).info();
        assertThat(anyInfo.ident().name(), is("any_="));
        //assertThat(anyInfo.ident().argumentTypes(), contains(DataTypes.LONG_ARRAY, DataType.LONG));

        analysis =  analyze("select * from users where 0 = ANY (counters)");
        assertThat(analysis.whereClause().hasQuery(), is(true));

        anyInfo = ((Function)analysis.whereClause().query()).info();
        assertThat(anyInfo.ident().name(), is("any_="));
        //assertThat(anyInfo.ident().argumentTypes(), contains(DataTypes.LONG_ARRAY, DataType.LONG));
    }

    @Test
    public void testArrayCompareAnyNeq() throws Exception {
        SelectAnalyzedStatement analysis = (SelectAnalyzedStatement) analyze("select * from users where ? != ANY (counters)",
                new Object[]{ 4.3F });
        assertThat(analysis.whereClause().hasQuery(), is(true));

        FunctionInfo anyInfo = ((Function)analysis.whereClause().query()).info();
        assertThat(anyInfo.ident().name(), is("any_<>"));
        //assertThat(anyInfo.ident().argumentTypes(), contains(DataTypes.LONG_ARRAY, DataType.LONG));

    }

    @Test(expected = UnsupportedFeatureException.class)
    public void testArrayCompareAll() throws Exception {
        analyze("select * from users where 0 = ALL (counters)");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testImplicitContainmentOnObjectArrayFields() throws Exception {
        // users.friends is an object array,
        // so its fields are selected as arrays,
        // ergo simple comparison does not work here
        analyze("select * from users where 5 = friends['id']");
    }

    @Test
    public void testAnyOnObjectArrayField() throws Exception {
        SelectAnalyzedStatement analysis = (SelectAnalyzedStatement) analyze(
                "select * from users where 5 = ANY (friends['id'])");
        assertThat(analysis.whereClause().hasQuery(), is(true));
        Function anyFunction = (Function)analysis.whereClause().query();
        assertThat(anyFunction.info().ident().name(), is(AnyEqOperator.NAME));

        Reference ref = (Reference) anyFunction.arguments().get(0);
        assertThat(ref.info().type().id(), is(ArrayType.ID));
        assertEquals(DataTypes.LONG, ((ArrayType)ref.info().type()).innerType());
        assertLiteralSymbol(anyFunction.arguments().get(1), 5L);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAnyOnArrayInObjectArray() throws Exception {
        analyze("select * from users where 'vogon lyric lovers' = ANY (friends['groups'])");
    }

    @Test
    public void testTableAlias() throws Exception {
        SelectAnalyzedStatement expectedAnalysis =  analyze("select * " +
                "from users where awesome = true");

        SelectAnalyzedStatement actualAnalysis =  analyze("select * " +
                "from users as u where u.awesome = true");
        SelectAnalyzedStatement actualAnalysisColAliased =  analyze("select awesome as a " +
                "from users as u where u.awesome = true");
        SelectAnalyzedStatement actualAnalysisOptionalAs =  analyze("select awesome a " +
                "from users u where u.awesome = true");

        assertEquals("u", actualAnalysis.tableAlias());
        assertEquals("u", actualAnalysisColAliased.tableAlias());
        assertEquals("u", actualAnalysisOptionalAs.tableAlias());
        assertEquals(
                ((Function)expectedAnalysis.whereClause().query()).arguments().get(0),
                ((Function)actualAnalysis.whereClause().query()).arguments().get(0)
        );
        assertEquals(
                ((Function)expectedAnalysis.whereClause().query()).arguments().get(0),
                ((Function)actualAnalysisColAliased.whereClause().query()).arguments().get(0)
        );
        assertEquals(
                ((Function) expectedAnalysis.whereClause().query()).arguments().get(0),
                ((Function) actualAnalysisOptionalAs.whereClause().query()).arguments().get(0)
        );
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testTableAliasWrongUse() throws Exception {
        analyze("select * from users as u where users.awesome = true");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testTableAliasFullQualifiedName() throws Exception {
        analyze("select * from users as u where doc.users.awesome = true");
    }

    @Test
    public void testAliasSubscript() throws Exception {
        SelectAnalyzedStatement analysis =  analyze(
                "select u.details['foo'] from users as u");
        assertThat(analysis.outputSymbols().size(), is(1));
        Symbol s = analysis.outputSymbols().get(0);
        assertThat(s, Matchers.instanceOf(Reference.class));
        Reference r = (Reference)s;
        assertThat(r.info().ident().tableIdent().name(), is("users"));
        assertThat(r.info().ident().columnIdent().fqn(), is("details.foo"));
    }

    @Test
    public void testOrderByWithOrdinal() throws Exception {
        SelectAnalyzedStatement analysis =  analyze(
                "select name from users u order by 1");
        assertEquals(analysis.outputSymbols().get(0), analysis.orderBy().orderBySymbols().get(0));
    }

    @Test
    public void testGroupWithIdx() throws Exception {
        SelectAnalyzedStatement analysis =  analyze(
                "select name from users u group by 1");
        assertEquals(analysis.outputSymbols().get(0), analysis.groupBy().get(0));
    }


    @Test
    public void testGroupWithInvalidIdx() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("GROUP BY position 2 is not in select list");
        analyze("select name from users u group by 2");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testOrderByOnArray() throws Exception {
        analyze("select * from users order by friends");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testOrderByOnObject() throws Exception {
        analyze("select * from sys.nodes order by load");
    }

    @Test
    public void testGroupByOnAnalyzed() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot select analyzed column 'users.text' within grouping or aggregations");
        analyze("select text from users u group by 1");
    }

    @Test
    public void testGroupByOnIndexOff() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot select non-indexed column 'users.no_index' within grouping or aggregations");
        analyze("select no_index from users u group by 1");
    }

    @Test
    public void testOrderByOnAnalyzed() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Cannot ORDER BY 'users.text': sorting on analyzed/fulltext columns is not possible");
        analyze("select text from users u order by 1");
    }

    @Test
    public void testOrderByOnIndexOff() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Cannot ORDER BY 'users.no_index': sorting on non-indexed columns is not possible");
        analyze("select no_index from users u order by 1");
    }

    @Test
    public void testArithmeticPlus() throws Exception {
        SelectAnalyzedStatement analysis = analyze("select load['1'] + load['5'] from sys.nodes");
        assertThat(((Function) analysis.outputSymbols().get(0)).info().ident().name(), is(AddFunction.NAME));
    }

    @Test
    public void testAnyLike() throws Exception {
        SelectAnalyzedStatement analysis =  analyze("select * from users where 'awesome' LIKE ANY (tags)");
        assertThat(analysis.whereClause().hasQuery(), is(true));
        Function query = (Function)analysis.whereClause().query();
        assertThat(query.info().ident().name(), is("any_like"));
        assertThat(query.arguments().size(), is(2));
        assertThat(query.arguments().get(0), Matchers.instanceOf(Reference.class));
        assertThat(((Reference) query.arguments().get(0)).info().ident().columnIdent().fqn(), is("tags"));
        assertThat(query.arguments().get(1), instanceOf(Literal.class));
        assertThat(((Literal<?>) query.arguments().get(1)).value(), Matchers.<Object>is(new BytesRef("awesome")));
    }

    @Test
    public void testAnyLikeLiteralMatchAll() throws Exception {
        SelectAnalyzedStatement analysis =  analyze("select * from users where 'awesome' LIKE ANY (['a', 'b', 'awesome'])");
        assertThat(analysis.whereClause().hasQuery(), is(false));
        assertThat(analysis.whereClause().noMatch(), is(false));
    }

    @Test
    public void testAnyLikeLiteralNoMatch() throws Exception {
        SelectAnalyzedStatement analysis =  analyze("select * from users where 'awesome' LIKE ANY (['a', 'b'])");
        assertThat(analysis.whereClause().hasQuery(), is(false));
        assertThat(analysis.whereClause().noMatch(), is(true));
    }

    @Test
    public void testAnyNotLike() throws Exception {
        SelectAnalyzedStatement analysis =  analyze("select * from users where 'awesome' NOT LIKE ANY (tags)");
        assertThat(analysis.whereClause().hasQuery(), is(true));
        Function query = (Function) analysis.whereClause().query();
        assertThat(query.info().ident().name(), is("any_not_like"));

        assertThat(query.arguments().size(), is(2));
        assertThat(query.arguments().get(0), Matchers.instanceOf(Reference.class));
        assertThat(((Reference) query.arguments().get(0)).info().ident().columnIdent().fqn(), is("tags"));
        assertThat(query.arguments().get(1), instanceOf(Literal.class));
        assertThat(((Literal<?>) query.arguments().get(1)).value(), Matchers.<Object>is(new BytesRef("awesome")));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAnyLikeInvalidArray() throws Exception {
        analyze("select * from users where 'awesome' LIKE ANY (name)");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPositionalArgumentGroupByArrayType() throws Exception {
        analyze("SELECT sum(id), friends FROM users GROUP BY 2");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testPositionalArgumentOrderByArrayType() throws Exception {
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

    private void testDistanceOrderBy(String stmt) throws Exception{
        SelectAnalyzedStatement analysis =  analyze(stmt);
        assertTrue(analysis.orderBy().isSorted());
        assertEquals(DistanceFunction.NAME, ((Function) analysis.orderBy().orderBySymbols().get(0)).info().ident().name());
    }

    @Test
    public void testSelectAnalyzedReferenceInFunction() throws Exception {
        SelectAnalyzedStatement analysis =  analyze("select substr(text, 0, 2) from users u");
        assertFalse(analysis.selectFromFieldCache);
    }

    @Test
    public void testSelectAnalyzedReferenceInFunctionGroupBy() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot select analyzed column 'users.text' within grouping or aggregations");
        analyze("select substr(text, 0, 2) from users u group by 1");
    }

    @Test
    public void testSelectAnalyzedReferenceInFunctionAggregation() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot select analyzed column 'users.text' within grouping or aggregations");
        analyze("select min(substr(text, 0, 2)) from users");
    }

    @Test
    public void testSelectNonIndexedReferenceInFunctionGroupBy() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot select non-indexed column 'users.no_index' within grouping or aggregations");
        analyze("select substr(no_index, 0, 2) from users u group by 1");
    }

    @Test
    public void testSelectNonIndexedReferenceInFunctionAggregation() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot select non-indexed column 'users.no_index' within grouping or aggregations");
        analyze("select min(substr(no_index, 0, 2)) from users");
    }

    @Test
    public void testWhereMatchOnColumn() throws Exception {
        SelectAnalyzedStatement analysis = analyze("select * from users where match(name, 'Arthur Dent')");
        Function query = (Function)analysis.whereClause.query();
        assertThat(query.info().ident().name(), is("match"));
        assertThat(query.arguments().size(), is(4));
        assertThat(query.arguments().get(0), Matchers.instanceOf(Literal.class));
        Literal<Map<String, Object>> idents = (Literal<Map<String, Object>>)query.arguments().get(0);

        assertThat(idents.value().size(), is(1));
        assertThat(idents.value().get("name"), is(nullValue()));

        assertThat(query.arguments().get(1), Matchers.instanceOf(Literal.class));
        assertThat((BytesRef)((Literal)query.arguments().get(1)).value(), is(new BytesRef("Arthur Dent")));
        assertThat((BytesRef)((Literal)query.arguments().get(2)).value(), is(new BytesRef("best_fields")));

        Literal<Map<String, Object>> options = (Literal<Map<String, Object>>)query.arguments().get(3);
        assertThat(options.value(), Matchers.instanceOf(Map.class));
        assertThat(options.value().size(), is(0));
    }

    @Test
    public void testMatchOnIndex() throws Exception {
        SelectAnalyzedStatement analysis =  analyze("select * from users where match(name_text_ft, 'Arthur Dent')");
        Function query = (Function) analysis.whereClause.query();
        assertThat(query.info().ident().name(), is("match"));
        assertThat(query.arguments().size(), is(4));
        assertThat(query.arguments().get(0), Matchers.instanceOf(Literal.class));
        Literal<Map<String, Object>> idents = (Literal<Map<String, Object>>)query.arguments().get(0);

        assertThat(idents.value().size(), is(1));
        assertThat(idents.value().get("name_text_ft"), is(nullValue()));

        assertThat(query.arguments().get(1), Matchers.instanceOf(Literal.class));
        assertThat((BytesRef)((Literal)query.arguments().get(1)).value(), is(new BytesRef("Arthur Dent")));
        assertThat((BytesRef)((Literal)query.arguments().get(2)).value(), is(new BytesRef("best_fields")));

        Literal<Map<String, Object>> options = (Literal<Map<String, Object>>)query.arguments().get(3);
        assertThat(options.value(), Matchers.instanceOf(Map.class));
        assertThat(options.value().size(), is(0));
    }

    @Test
    public void testMatchOnDynamicColumn() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("cannot MATCH on non existing column users.details['me_not_exizzt']");

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
    public void testSelectWhereSimpleMatchPredicate() throws Exception {
        SelectAnalyzedStatement analysis =  analyze("select * from users where match (text, 'awesome')");
        assertThat(analysis.whereClause().hasQuery(), is(true));
        Function query = (Function)analysis.whereClause().query();
        assertThat(query.info().ident().name(), is(MatchPredicate.NAME));
        assertThat(query.arguments().size(), is(4));
        assertThat(query.arguments().get(0), Matchers.instanceOf(Literal.class));
        Literal<Map<String, Object>> idents = (Literal<Map<String, Object>>)query.arguments().get(0);

        assertThat(idents.value().keySet(), hasItem("text"));
        assertThat(idents.value().get("text"), is(nullValue()));
        assertThat(query.arguments().get(1), instanceOf(Literal.class));
        assertThat(((Literal<?>) query.arguments().get(1)).value(), Matchers.<Object>is(new BytesRef("awesome")));
    }

    @Test
    public void testSelectWhereFullMatchPredicate() throws Exception {
        SelectAnalyzedStatement analysis =  analyze("select * from users " +
                "where match ((name 1.2, text), 'awesome') using best_fields with (analyzer='german')");
        assertThat(analysis.whereClause().hasQuery(), is(true));
        Function query = (Function)analysis.whereClause().query();
        assertThat(query.info().ident().name(), is(MatchPredicate.NAME));
        assertThat(query.arguments().size(), is(4));
        assertThat(query.arguments().get(0), Matchers.instanceOf(Literal.class));
        Literal<Map<String, Object>> idents = (Literal<Map<String, Object>>)query.arguments().get(0);

        assertThat(idents.value().size(), is(2));
        assertThat((Double)idents.value().get("name"), is(1.2d));
        assertThat(idents.value().get("text"), is(Matchers.nullValue()));

        assertThat((BytesRef)((Literal)query.arguments().get(1)).value(), is(new BytesRef("awesome")));
        assertThat((BytesRef)((Literal)query.arguments().get(2)).value(), is(new BytesRef("best_fields")));

        Literal<Map<String, Object>> options = (Literal<Map<String, Object>>)query.arguments().get(3);
        assertThat(options.value().size(), is(1));
        assertThat((String)options.value().get("analyzer"), is("german"));
    }

    @Test
    public void testWhereFullMatchPredicateNullQuery() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("query_term is not a string nor a parameter");
        analyze("select * from users " +
                "where match ((name 1.2, text), null) using best_fields with (analyzer='german')");
    }

    @Test
    public void testWhereMatchUnknownType() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("invalid MATCH type 'some_fields'");
        analyze("select * from users " +
                "where match ((name 1.2, text), 'awesome') using some_fields");
    }

    @Test
    public void testWhereMatchUnknownOption() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("unknown match option 'oh'");
        analyze("select * from users " +
                "where match ((name 4.777, text), 'awesome') using cross_fields with (oh='srsly?')");
    }

    @Test
    public void testWhereMatchInvalidOptionValue() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("invalid value for option 'zero_terms_query': 12.6");
        analyze("select * from users " +
                "where match ((name 4.777, text), 'awesome') using cross_fields with (zero_terms_query=12.6)");
    }

    private String getMatchType(Function matchFunction) {
        return ((BytesRef)((Literal)matchFunction.arguments().get(2)).value()).utf8ToString();
    }

    @Test
    public void testWhereMatchAllowedTypes() throws Exception {
        SelectAnalyzedStatement best_fields_analysis = analyze("select * from users " +
                "where match ((name 1.2, text), 'awesome') using best_fields");
        SelectAnalyzedStatement most_fields_analysis = analyze("select * from users " +
                "where match ((name 1.2, text), 'awesome') using most_fields");
        SelectAnalyzedStatement cross_fields_analysis = analyze("select * from users " +
                "where match ((name 1.2, text), 'awesome') using cross_fields");
        SelectAnalyzedStatement phrase_analysis = analyze("select * from users " +
                "where match ((name 1.2, text), 'awesome') using phrase");
        SelectAnalyzedStatement phrase_prefix_analysis = analyze("select * from users " +
                "where match ((name 1.2, text), 'awesome') using phrase_prefix");


        assertThat(getMatchType((Function)best_fields_analysis.whereClause.query()),
                is("best_fields"));
        assertThat(getMatchType((Function)most_fields_analysis.whereClause.query()),
                is("most_fields"));
        assertThat(getMatchType((Function) cross_fields_analysis.whereClause.query()),
                is("cross_fields"));
        assertThat(getMatchType((Function) phrase_analysis.whereClause.query()),
                is("phrase"));
        assertThat(getMatchType((Function) phrase_prefix_analysis.whereClause.query()),
                is("phrase_prefix"));
    }

    @Test
    public void testWhereMatchAllOptions() throws Exception {
        SelectAnalyzedStatement analysis = analyze("select * from users " +
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
        Function match = (Function)analysis.whereClause.query();
        Map<String, Object> options = ((Literal<Map<String, Object>>)match.arguments().get(3)).value();
        assertThat(Joiner.on(", ").withKeyValueSeparator(":").join(options),
                is("zero_terms_query:all, cutoff_frequency:5, minimum_should_match:4, " +
                        "rewrite:constant_score_boolean, prefix_length:4, tie_breaker:0.75, " +
                        "slop:3, analyzer:german, boost:4.6, max_expansions:3, fuzzy_rewrite:top_terms_20, " +
                        "fuzziness:12, operator:or"));
    }

    @Test
    public void testIsNotNullDynamic() {
         SelectAnalyzedStatement analysis = analyze("select * from users where o['no_such_column'] is not null");
         assertTrue(analysis.hasNoResult());
    }

    @Test
    public void testGroupByHaving() throws Exception {
        SelectAnalyzedStatement analysis = analyze("select sum(floats) from users group by name having name like 'Slartibart%'");
        assertThat(analysis.havingClause(), isFunction("op_like"));
        Function havingFunction = (Function)analysis.havingClause();
        assertThat(havingFunction.arguments().size(), is(2));
        assertThat(havingFunction.arguments().get(0), isReference("name"));
        TestingHelpers.assertLiteralSymbol(havingFunction.arguments().get(1), "Slartibart%");
    }

    @Test
    public void testGroupByHavingNormalize() throws Exception {
        SelectAnalyzedStatement analysis = analyze("select sum(floats) from users group by name having 1 > 4");
        TestingHelpers.assertLiteralSymbol(analysis.havingClause(), false);
    }

    @Test
    public void testGroupByHavingOtherColumnInAggregate() throws Exception {
        SelectAnalyzedStatement analysis = analyze("select sum(floats), name from users group by name having max(bytes) = 4");
        assertThat(analysis.havingClause(), isFunction("op_="));
        Function havingFunction = (Function)analysis.havingClause();
        assertThat(havingFunction.arguments().size(), is(2));
        assertThat(havingFunction.arguments().get(0), isFunction("max"));
        Function maxFunction = (Function)havingFunction.arguments().get(0);

        assertThat(maxFunction.arguments().get(0), isReference("bytes"));
        TestingHelpers.assertLiteralSymbol(havingFunction.arguments().get(1), (byte) 4, DataTypes.BYTE);
    }

    @Test
    public void testGroupByHavingOtherColumnOutsideAggregate() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot use reference users.bytes outside of an Aggregation in HAVING clause");

        analyze("select sum(floats) from users group by name having bytes = 4");
    }

    @Test
    public void testGroupByHavingOtherColumnOutsideAggregateInFunction() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot use reference users.bytes outside of an Aggregation in HAVING clause");

        analyze("select sum(floats), name from users group by name having (bytes + 1)  = 4");
    }

    @Test
    public void testGroupByHavingByGroupKey() throws Exception {
        SelectAnalyzedStatement analysis = analyze(
                "select sum(floats), name from users group by name having name like 'Slartibart%'");
        assertThat(analysis.havingClause(), isFunction("op_like"));
        Function havingFunction = (Function)analysis.havingClause();
        assertThat(havingFunction.arguments().size(), is(2));
        assertThat(havingFunction.arguments().get(0), TestingHelpers.isReference("name"));
        TestingHelpers.assertLiteralSymbol(havingFunction.arguments().get(1), "Slartibart%");
    }

    @Test
    public void testGroupByHavingComplex() throws Exception {
        SelectAnalyzedStatement analysis = analyze("select sum(floats), name from users " +
                "group by name having 1=0 or sum(bytes) in (42, 43, 44) and  name not like 'Slartibart%'");
        assertThat(analysis.havingClause(), instanceOf(Function.class));
        Function andFunction = (Function)analysis.havingClause();
        assertThat(andFunction, is(notNullValue()));
        assertThat(andFunction.info().ident().name(), is("op_and"));
        assertThat(andFunction.arguments().size(), is(2));

        assertThat(andFunction.arguments().get(0), isFunction("op_in"));
        assertThat(andFunction.arguments().get(1), isFunction("op_not"));
    }

    @Test
    public void testHavingWithoutGroupBy() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("HAVING clause can only be used in GROUP BY or global aggregate queries");
        analyze("select * from users having max(bytes) > 100");
    }

    @Test
    public void testGlobalAggregateHaving() throws Exception {
        SelectAnalyzedStatement analysis = analyze("select sum(floats) from users having sum(bytes) in (42, 43, 44)");
        assertThat(analysis.havingClause(), isFunction("op_in"));
        Function havingFunction = (Function)analysis.havingClause();

        assertThat(havingFunction.info().ident().name(), is("op_in"));
        assertThat(havingFunction.arguments().size(), is(2));

        assertThat(havingFunction.arguments().get(0), isFunction("sum"));
        TestingHelpers.assertLiteralSymbol(havingFunction.arguments().get(1), Sets.newHashSet(42.0D, 43.0D, 44.0D), new SetType(DataTypes.DOUBLE));

    }

    @Test
    public void testHavingNoResult() throws Exception {
        SelectAnalyzedStatement analysis = analyze("select sum(floats) from users having 1 = 2");
        assertThat(analysis.havingClause(), isLiteral(false, DataTypes.BOOLEAN));
        assertThat(analysis.hasNoResult(), is(true));
    }

    @Test
    public void testGlobalAggregateReference() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot use reference users.bytes outside of an Aggregation in HAVING clause. Only GROUP BY keys allowed here.");

        analyze("select sum(floats) from users having bytes in (42, 43, 44)");
    }

    @Test
    public void testScoreReferenceInvalidComparison() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("System column '_score' can only be used within a '>=' comparison without any surrounded predicate");
        analyze("select * from users where \"_score\" = 0.9");
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
        expectedException.expectMessage("System column '_score' cannot be used within a predicate");
        analyze("select * from users where \"_score\" in (0.9)");
    }

    @Test
    public void testScoreReferenceInvalidNullPredicate() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("System column '_score' cannot be used within a predicate");
        analyze("select * from users where \"_score\" is null");
    }

    @Test
    public void testScoreReferenceInvalidNotNullPredicate() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("System column '_score' cannot be used within a predicate");
        analyze("select * from users where \"_score\" is not null");
    }

    @Test
    public void testRegexpMatchInvalidArg() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("type of \"users.floats\" doesn't match type of \"'foo'\" and cannot be cast implicitly");
        analyze("select * from users where floats ~ 'foo'");
    }

    @Test
    public void testRegexpMatchNull() throws Exception {
        SelectAnalyzedStatement analysis = analyze("select * from users where name ~ null");
        assertThat(analysis.whereClause().hasQuery(), is(false));
        assertThat(analysis.whereClause().noMatch(), is(true));
    }

    @Test
    public void testRegexpMatch() throws Exception {
        SelectAnalyzedStatement analysis = analyze("select * from users where name ~ '.*foo(bar)?'");
        assertThat(analysis.whereClause().hasQuery(), is(true));
        assertThat(((Function) analysis.whereClause().query()).info().ident().name(), is("op_~"));
    }

    @Test
    public void testSubscriptArray() throws Exception {
        SelectAnalyzedStatement analysis = analyze("select tags[1] from users");
        assertThat(analysis.outputSymbols().get(0), isFunction(SubscriptFunction.NAME));
        List<Symbol> arguments = ((Function) analysis.outputSymbols().get(0)).arguments();
        assertThat(arguments.size(), is(2));
        assertThat(arguments.get(0), isReference("tags"));
        assertThat(arguments.get(1), isLiteral(0, DataTypes.INTEGER));
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
        SelectAnalyzedStatement analysis = analyze("select tags[1]['name'] from deeply_nested");
        assertThat(analysis.outputSymbols().get(0), isFunction(SubscriptFunction.NAME));
        List<Symbol> arguments = ((Function) analysis.outputSymbols().get(0)).arguments();
        assertThat(arguments.size(), is(2));
        assertThat(arguments.get(0), isReference("tags.name"));
        assertThat(arguments.get(1), isLiteral(0, DataTypes.INTEGER));
    }

    @Test
    public void testSubscriptArrayInvalidNesting() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Nested array access is not supported");
        analyze("select tags[1]['metadata'][2] from deeply_nested");
    }

    @Test
    public void testSubscriptArrayAsAlias() throws Exception {
        SelectAnalyzedStatement analysis = analyze("select tags[1] as t_alias from users");
        assertThat(analysis.outputSymbols().get(0), isFunction(SubscriptFunction.NAME));
        List<Symbol> arguments = ((Function) analysis.outputSymbols().get(0)).arguments();
        assertThat(arguments.size(), is(2));
        assertThat(arguments.get(0), isReference("tags"));
        assertThat(arguments.get(1), isLiteral(0, DataTypes.INTEGER));
    }

    @Test
    public void testSubscriptArrayOnScalarResult() throws Exception {
        SelectAnalyzedStatement analysis = analyze("select regexp_matches(name, '.*')[1] as t_alias from users order by t_alias");
        assertThat(analysis.outputSymbols().get(0), isFunction(SubscriptFunction.NAME));
        assertThat(analysis.orderBy().orderBySymbols().get(0), is(analysis.outputSymbols().get(0)));
        List<Symbol> arguments = ((Function) analysis.outputSymbols().get(0)).arguments();
        assertThat(arguments.size(), is(2));

        assertThat(arguments.get(0), isFunction(MatchesFunction.NAME));
        assertThat(arguments.get(1), isLiteral(0, DataTypes.INTEGER));

        List<Symbol> scalarArguments = ((Function) arguments.get(0)).arguments();
        assertThat(scalarArguments.size(), is(2));
        assertThat(scalarArguments.get(0), isReference("name"));
        assertThat(scalarArguments.get(1), isLiteral(".*", DataTypes.STRING));
    }

    @Test
    public void testNormalizedSubscriptOnArrayLiteral() throws Exception {
        SelectAnalyzedStatement analysis;
        for (long l = 1L; l<4; l++) {
            analysis = analyze("select [1,2,3][?] from sys.cluster", new Object[]{l});
            assertThat(analysis.outputSymbols().get(0), isLiteral(l, DataTypes.LONG));
        }
    }

    @Test
    public void testParameterSubcript() throws Exception {
        SelectAnalyzedStatement analysis = analyze("select friends[?], counters[?], ['a','b','c'][?] from users",
                new Object[]{"id",2,3});
        assertThat(analysis.outputSymbols().get(0), isReference("friends.id", new ArrayType(DataTypes.LONG)));
        assertThat(analysis.outputSymbols().get(1), isFunction(SubscriptFunction.NAME,
                Arrays.<DataType>asList(new ArrayType(DataTypes.LONG), DataTypes.INTEGER)));
        assertThat(analysis.outputSymbols().get(2), isLiteral("c", DataTypes.STRING));
    }


    @Test
    public void testClusterRefAndPartitionInWhereClause() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage(
                "Selecting system columns from regular tables is currently only supported by queries using group-by or global aggregates.");
        analyze("select * from parted " +
                "where sys.cluster.name = 'foo' and date = 1395874800000");
    }

    @Test
    public void testCastExpression() throws Exception {
        SelectAnalyzedStatement analysis = analyze("select cast(other_id as string) from users");
        assertThat(analysis.outputSymbols().get(0), isFunction(ToStringFunction.NAME,
                Arrays.<DataType>asList(DataTypes.LONG)));

        analysis = analyze("select cast(1+1 as string) from users");
        assertThat(analysis.outputSymbols().get(0), isLiteral("2", DataTypes.STRING));

        analysis = analyze("select cast(friends['id'] as array(string)) from users");
        assertThat(analysis.outputSymbols().get(0), isFunction(ToStringArrayFunction.NAME,
                Arrays.<DataType>asList(new ArrayType(DataTypes.LONG))));
    }

    @Test
    public void testInvalidCastExpression() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("No cast function found for return type object");
        analyze("select cast(name as object) from users");
    }

    @Test
    public void testSelectWithAliasRenaming() throws Exception {
        SelectAnalyzedStatement analysis = analyze("select text as name, name as n from users");

        Symbol text = analysis.outputSymbols().get(0);
        Symbol name = analysis.outputSymbols().get(1);

        assertThat(text, isReference("text"));
        assertThat(name, isReference("name"));
    }

    @Test
    public void testFunctionArgumentsCantBeAliases() throws Exception {
        expectedException.expect(ColumnUnknownException.class);
        expectedException.expectMessage("Column 'n' unknown");
        analyze("select name as n, substr(n, 1, 1) from users");
    }

    @Test
    public void testSubscriptOnAliasShouldntWork() throws Exception {
        expectedException.expect(ColumnUnknownException.class);
        expectedException.expectMessage("Column 'n' unknown");
        analyze("select name as n, n[1] from users");
    }

    @Test
    public void testCanSelectColumnWithAndWithoutSubscript() throws Exception {
        SelectAnalyzedStatement analysis = analyze("select counters, counters[1] from users");
        Symbol counters = analysis.outputSymbols().get(0);
        Symbol countersSubscript = analysis.outputSymbols().get(1);

        assertThat(counters, isReference("counters"));
        assertThat(countersSubscript, isFunction("subscript"));
    }

    @Test
    public void testOrderByOnAliasWithSameColumnNameInSchema() throws Exception {
        // name exists in the table but isn't selected so not ambiguous
        SelectAnalyzedStatement analysis = analyze("select other_id as name from users order by name");
        assertThat(analysis.outputSymbols().get(0), isReference("other_id"));
        List<Symbol> sortSymbols = analysis.orderBy().orderBySymbols();
        assert sortSymbols != null;
        assertThat(sortSymbols.get(0), isReference("other_id"));
    }

    @Test
    public void testEmptyClusteredByValue() throws Exception {
        SelectAnalyzedStatement analysis = analyze("select * from bystring where name = ''");
        assertThat(analysis.whereClause().clusteredBy().get(), is(""));
        assertThat(analysis.ids().size(), is(1));
        assertThat(analysis.ids().get(0), is(""));
    }

    @Test
    public void testClusteredByValueContainsComma() throws Exception {
        SelectAnalyzedStatement analysis = analyze("select * from bystring where name = 'a,b,c'");
        assertThat(analysis.whereClause().clusteredBy().get(), is("a,b,c"));
        assertThat(analysis.ids().size(), is(1));
        assertThat(analysis.ids().get(0), is("a,b,c"));
    }

    @Test
    public void testShardGranularityFromNodeGranularityTable() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Cannot resolve reference 'sys.shards.id', reason: finer granularity than table 'sys.nodes'");
        analyze("select sys.shards.id from sys.nodes");
    }
}
