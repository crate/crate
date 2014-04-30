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
import io.crate.DataType;
import io.crate.PartitionName;
import io.crate.exceptions.AmbiguousAliasException;
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
import io.crate.operation.predicate.IsNullPredicate;
import io.crate.operation.predicate.NotPredicate;
import io.crate.operation.predicate.PredicateModule;
import io.crate.operation.reference.sys.node.NodeLoadExpression;
import io.crate.operation.scalar.CollectionCountFunction;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.*;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.inject.Module;
import org.hamcrest.Matchers;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Test;

import java.util.*;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class SelectAnalyzerTest extends BaseAnalyzerTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

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
            when(schemaInfo.getTableInfo(TEST_PARTITIONED_TABLE_IDENT.name()))
                    .thenReturn(TEST_PARTITIONED_TABLE_INFO);
            when(schemaInfo.getTableInfo(TEST_MULTIPLE_PARTITIONED_TABLE_IDENT.name()))
                    .thenReturn(TEST_MULTIPLE_PARTITIONED_TABLE_INFO);
            schemaBinder.addBinding(DocSchemaInfo.NAME).toInstance(schemaInfo);
        }

        @Override
        protected void bindFunctions() {
            super.bindFunctions();
            functionBinder.addBinding(ABS_FUNCTION_INFO.ident())
                    .toInstance(new AbsFunction());
            functionBinder.addBinding(YEAR_FUNCTION_INFO.ident()).toInstance(new YearFunction());
        }
    }

    @Override
    protected List<Module> getModules() {
        List<Module> modules = super.getModules();
        modules.addAll(Arrays.<Module>asList(
                new TestModule(),
                new TestMetaDataModule(),
                new MetaDataSysModule(),
                new OperatorModule(),
                new AggregationImplModule(),
                new PredicateModule(),
                new ScalarFunctionModule()
        ));
        return modules;
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGroupedSelectMissingOutput() throws Exception {
        analyze("select load['5'] from sys.nodes group by load['1']");
    }

    @Test
    public void testIsNullQuery() {
        SelectAnalysis analysis = (SelectAnalysis)analyze("select * from sys.nodes where id is not null");
        assertTrue(analysis.whereClause().hasQuery());
        Function query = (Function)analysis.whereClause().query();

        assertThat(query.info().ident().name(), is(NotPredicate.NAME));
        assertThat(query.arguments().get(0), instanceOf(Function.class));
        Function isNull = (Function) query.arguments().get(0);
        assertThat(isNull.info().ident().name(), is(IsNullPredicate.NAME));
    }

    @Test
    public void testOrderedSelect() throws Exception {
        SelectAnalysis analysis = (SelectAnalysis) analyze("select load['1'] from sys.nodes order by load['5'] desc");
        assertEquals(analysis.table().ident(), SysNodesTableInfo.IDENT);
        assertNull(analysis.limit());

        assertFalse(analysis.hasGroupBy());
        assertTrue(analysis.isSorted());
        assertThat(analysis.rowGranularity(), is(RowGranularity.NODE));

        assertEquals(1, analysis.outputSymbols().size());
        assertEquals(1, analysis.sortSymbols().size());
        assertEquals(1, analysis.reverseFlags().length);

        assertEquals(LOAD5_INFO, ((Reference) analysis.sortSymbols().get(0)).info());
    }

    @Test
    public void testGroupKeyNotInResultColumnList() throws Exception {
        SelectAnalysis analysis = (SelectAnalysis) analyze("select count(*) from sys.nodes group by name");

        assertThat(analysis.groupBy().size(), is(1));
        assertThat(analysis.outputNames().get(0), is("count(*)"));
    }

    @Test
    public void testGroupByOnAlias() throws Exception {
        SelectAnalysis analysis = (SelectAnalysis) analyze("select count(*), name as n from sys.nodes group by n");
        assertThat(analysis.groupBy().size(), is(1));
        assertThat(analysis.outputNames().get(0), is("count(*)"));
        assertThat(analysis.outputNames().get(1), is("n"));

        assertEquals(analysis.groupBy().get(0), analysis.outputSymbols().get(1));
    }

    @Test
    public void testGroupByOnOrdinal() throws Exception {
        // just like in postgres access by ordinal starts with 1
        SelectAnalysis analysis = (SelectAnalysis) analyze("select count(*), name as n from sys.nodes group by 2");
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
        SelectAnalysis analyze = (SelectAnalysis)analyze("select * from sys.nodes where port['http'] = -400");
        Function whereClause = (Function)analyze.whereClause().query();
        Symbol symbol = whereClause.arguments().get(1);
        assertThat(((IntegerLiteral) symbol).value(), is(-400));
    }

    @Test
    public void testGroupedSelect() throws Exception {
        SelectAnalysis analysis = (SelectAnalysis) analyze("select load['1'], count(*) from sys.nodes group by load['1']");
        assertEquals(analysis.table().ident(), SysNodesTableInfo.IDENT);
        assertNull(analysis.limit());

        assertThat(analysis.rowGranularity(), is(RowGranularity.NODE));
        assertTrue(analysis.hasGroupBy());
        assertEquals(2, analysis.outputSymbols().size());
        assertEquals(1, analysis.groupBy().size());
        assertEquals(LOAD1_INFO, ((Reference) analysis.groupBy().get(0)).info());

    }


    @Test
    public void testSimpleSelect() throws Exception {
        SelectAnalysis analysis = (SelectAnalysis) analyze("select load['5'] from sys.nodes limit 2");
        assertEquals(analysis.table().ident(), SysNodesTableInfo.IDENT);
        assertEquals(new Integer(2), analysis.limit());

        assertFalse(analysis.hasGroupBy());

        assertThat(analysis.rowGranularity(), is(RowGranularity.NODE));

        assertEquals(SysNodesTableInfo.IDENT, analysis.table().ident());
        assertEquals(1, analysis.outputSymbols().size());
        Reference col1 = (Reference) analysis.outputSymbols().get(0);
        assertEquals(LOAD5_INFO, col1.info());

    }

    @Test
    public void testAggregationSelect() throws Exception {
        SelectAnalysis analysis = (SelectAnalysis) analyze("select avg(load['5']) from sys.nodes");
        assertEquals(SysNodesTableInfo.IDENT, analysis.table().ident());

        assertThat(analysis.rowGranularity(), is(RowGranularity.NODE));

        assertFalse(analysis.hasGroupBy());
        assertEquals(1, analysis.outputSymbols().size());
        Function col1 = (Function) analysis.outputSymbols().get(0);
        assertTrue(col1.info().isAggregate());
        assertEquals(AverageAggregation.NAME, col1.info().ident().name());
    }

    @Test
    public void testAllColumnCluster() throws Exception {
        SelectAnalysis analysis = (SelectAnalysis)analyze("select * from sys.cluster");
        assertThat(analysis.outputNames().size(), is(2));
        assertThat(analysis.outputNames().get(0), is("id"));
        assertThat(analysis.outputNames().get(1), is("name"));

        assertThat(analysis.outputSymbols().size(), is(2));
    }

    @Test
    public void testAllColumnNodes() throws Exception {
        SelectAnalysis analysis = (SelectAnalysis)analyze("select id, * from sys.nodes");
        assertThat(analysis.outputNames().get(0), is("id"));
        assertThat(analysis.outputNames().get(1), is("id"));
        assertThat(analysis.outputNames().size(), is(10));
        assertEquals(analysis.outputNames().size(), analysis.outputSymbols().size());
    }

    @Test
    public void testWhereSelect() throws Exception {
        SelectAnalysis analysis = (SelectAnalysis) analyze("select load from sys.nodes " +
                "where load['1'] = 1.2 or 1 >= load['5']");
        assertEquals(SysNodesTableInfo.IDENT, analysis.table().ident());

        assertThat(analysis.rowGranularity(), is(RowGranularity.NODE));

        assertFalse(analysis.hasGroupBy());

        Function whereClause = (Function)analysis.whereClause().query();
        assertEquals(OrOperator.NAME, whereClause.info().ident().name());
        assertFalse(whereClause.info().isAggregate());

        Function left = (Function) whereClause.arguments().get(0);
        assertEquals(EqOperator.NAME, left.info().ident().name());
        assertThat(left.arguments().get(0), IsInstanceOf.instanceOf(Reference.class));
        assertThat(left.arguments().get(1), IsInstanceOf.instanceOf(DoubleLiteral.class));

        Function right = (Function) whereClause.arguments().get(1);
        assertEquals(LteOperator.NAME, right.info().ident().name());
        assertThat(right.arguments().get(0), IsInstanceOf.instanceOf(Reference.class));
        assertThat(right.arguments().get(1), IsInstanceOf.instanceOf(DoubleLiteral.class));
    }

    @Test
    public void testSelectWithParameters() throws Exception {
        SelectAnalysis analysis = (SelectAnalysis)analyze("select load from sys.nodes " +
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
        assertFalse(whereClause.info().isAggregate());

        Function function = (Function) whereClause.arguments().get(0);
        assertEquals(OrOperator.NAME, function.info().ident().name());
        function = (Function) function.arguments().get(1);
        assertEquals(EqOperator.NAME, function.info().ident().name());
        assertThat(function.arguments().get(0), IsInstanceOf.instanceOf(Reference.class));
        assertThat(function.arguments().get(1), IsInstanceOf.instanceOf(DoubleLiteral.class));

        function = (Function) whereClause.arguments().get(1);
        assertEquals(EqOperator.NAME, function.info().ident().name());
        assertThat(function.arguments().get(0), IsInstanceOf.instanceOf(Reference.class));
        assertThat(function.arguments().get(1), IsInstanceOf.instanceOf(StringLiteral.class));
    }

    @Test
    public void testOutputNames() throws Exception {
        SelectAnalysis analysis = (SelectAnalysis)analyze("select load as l, id, load['1'] from sys.nodes");
        assertThat(analysis.outputNames().size(), is(3));
        assertThat(analysis.outputNames().get(0), is("l"));
        assertThat(analysis.outputNames().get(1), is("id"));
        assertThat(analysis.outputNames().get(2), is("load['1']"));
    }

    @Test
    public void testDuplicateOutputNames() throws Exception {
        SelectAnalysis analysis = (SelectAnalysis)analyze("select load as l, load['1'] as l from sys.nodes");
        assertThat(analysis.outputNames().size(), is(2));
        assertThat(analysis.outputNames().get(0), is("l"));
        assertThat(analysis.outputNames().get(1), is("l"));
    }

    @Test
    public void testOrderByOnAlias() throws Exception {
        SelectAnalysis analyze = (SelectAnalysis) analyze("select load as l from sys.nodes order by l");
        assertThat(analyze.outputNames().size(), is(1));
        assertThat(analyze.outputNames().get(0), is("l"));

        assertTrue(analyze.isSorted());
        assertThat(analyze.sortSymbols().size(), is(1));
        assertThat(analyze.sortSymbols().get(0), is(analyze.outputSymbols().get(0)));
    }

    @Test(expected = AmbiguousAliasException.class)
    public void testAmbiguousOrderByOnAlias() throws Exception {
        analyze("select id as load, load from sys.nodes order by load");
    }

    @Test
    public void testOffsetSupportInAnalyzer() throws Exception {
        SelectAnalysis analyze = (SelectAnalysis) analyze("select * from sys.nodes limit 1 offset 3");
        assertThat(analyze.offset(), is(3));
    }

    @Test
    public void testNoMatchStatement() throws Exception {
        for (String stmt : ImmutableList.of(
                "select id from sys.nodes where false",
                "select id from sys.nodes where 1=0",
                "select id from sys.nodes where sys.cluster.name = 'something'"
        )) {
            SelectAnalysis analysis = (SelectAnalysis)analyze(stmt);
            assertTrue(stmt, analysis.noMatch());
            assertFalse(stmt, analysis.whereClause().hasQuery());
        }
    }

    @Test
    public void testEvaluatingMatchAllStatement() throws Exception {
        SelectAnalysis analysis = (SelectAnalysis)analyze("select id from sys.nodes where sys.cluster.name = 'testcluster'");
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
            SelectAnalysis analysis = (SelectAnalysis)analyze(stmt);
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
            SelectAnalysis analysis = (SelectAnalysis)analyze(statement);
            WhereClause whereClause = analysis.whereClause();

            Function notFunction = (Function)whereClause.query();
            assertThat(notFunction.info().ident().name(), is(NotPredicate.NAME));
            assertThat(notFunction.arguments().size(), is(1));

            Function eqFunction = (Function) notFunction.arguments().get(0);
            assertThat(eqFunction.info().ident().name(), is(EqOperator.NAME));
            assertThat(eqFunction.arguments().size(), is(2));

            List<Symbol> eqArguments = eqFunction.arguments();
            assertThat(eqArguments.get(0), instanceOf(Reference.class));
            assertThat(eqArguments.get(1), instanceOf(StringLiteral.class));
        }
    }

    @Test
    public void testClusteredBy() throws Exception {
        SelectAnalysis analysis = (SelectAnalysis)analyze("select name from users where id=1");
        assertEquals(ImmutableList.of("1"), analysis.routingValues());
        assertEquals("1", analysis.whereClause().clusteredBy().get());

        analysis = (SelectAnalysis)analyze("select name from users where id=1 or id=2");
        assertEquals(ImmutableList.of("1", "2"), analysis.routingValues());
        assertFalse(analysis.whereClause().clusteredBy().isPresent());
    }

    @Test
    public void testClusteredByOnly() throws Exception {
        SelectAnalysis analysis = (SelectAnalysis)analyze("select name from users_clustered_by_only where id=1");
        assertEquals(ImmutableList.of(), analysis.ids());
        assertEquals(ImmutableList.of("1"), analysis.routingValues());
        assertEquals("1", analysis.whereClause().clusteredBy().get());

        analysis = (SelectAnalysis)analyze("select name from users_clustered_by_only where id=1 or id=2");
        assertEquals(ImmutableList.of(), analysis.ids());
        assertEquals(ImmutableList.of(), analysis.routingValues());
        assertFalse(analysis.whereClause().clusteredBy().isPresent());

        analysis = (SelectAnalysis)analyze("select name from users_clustered_by_only where id=1 and id=2");
        assertEquals(ImmutableList.of(), analysis.ids());
        assertEquals(ImmutableList.of(), analysis.routingValues());
        assertFalse(analysis.whereClause().clusteredBy().isPresent());
    }

    @Test
    public void testCompositePrimaryKey() throws Exception {
        SelectAnalysis analysis = (SelectAnalysis)analyze("select name from users_multi_pk where id=1");
        assertEquals(ImmutableList.of(), analysis.ids());
        assertEquals(ImmutableList.of(), analysis.routingValues());
        assertEquals("1", analysis.whereClause().clusteredBy().get());

        analysis = (SelectAnalysis)analyze("select name from users_multi_pk where id=1 and name='Douglas'");
        assertEquals(ImmutableList.of("AgExB0RvdWdsYXM="), analysis.ids());
        assertEquals(ImmutableList.of("1"), analysis.routingValues());
        assertEquals("1", analysis.whereClause().clusteredBy().get());

        analysis = (SelectAnalysis)analyze("select name from users_multi_pk where id=1 or id=2 and name='Douglas'");
        assertEquals(ImmutableList.of(), analysis.ids());
        assertEquals(ImmutableList.of(), analysis.routingValues());
        assertFalse(analysis.whereClause().clusteredBy().isPresent());

        analysis = (SelectAnalysis)analyze("select name from users_multi_pk where id=1 and name='Douglas' or name='Arthur'");
        assertEquals(ImmutableList.of(), analysis.ids());
        assertEquals(ImmutableList.of(), analysis.routingValues());
        assertFalse(analysis.whereClause().clusteredBy().isPresent());
    }

    @Test
    public void testPrimaryKeyAndVersion() throws Exception {
        SelectAnalysis analysis = (SelectAnalysis)analyze(
            "select name from users where id = 2 and \"_version\" = 1");
        assertEquals(ImmutableList.of("2"), analysis.ids());
        assertEquals(ImmutableList.of("2"), analysis.routingValues());
        assertThat(analysis.whereClause().version().get(), is(1L));
    }

    @Test
    public void testMultiplePrimaryKeys() throws Exception {
        SelectAnalysis analysis = (SelectAnalysis)analyze(
            "select name from users where id = 2 or id = 1");

        assertEquals(ImmutableList.of("1", "2"), analysis.ids());
        assertEquals(ImmutableList.of("1", "2"), analysis.routingValues());
    }

    @Test
    public void testMultiplePrimaryKeysAndInvalidColumn() throws Exception {
        SelectAnalysis analysis = (SelectAnalysis)analyze(
            "select name from users where id = 2 or id = 1 and name = 'foo'");
        assertEquals(0, analysis.ids().size());
    }

    @Test
    public void testNotEqualsDoesntMatchPrimaryKey() throws Exception {
        SelectAnalysis analysis = (SelectAnalysis)analyze("select name from users where id != 1");
        assertEquals(0, analysis.ids().size());
        assertEquals(0, analysis.routingValues().size());
    }

    @Test
    public void testMultipleCompoundPrimaryKeys() throws Exception {
        SelectAnalysis analysis = (SelectAnalysis)analyze(
            "select * from sys.shards where (schema_name='doc' and id = 1 and table_name = 'foo' and partition_ident='') " +
                    "or (schema_name='doc' and id = 2 and table_name = 'bla' and partition_ident='')");
        assertEquals(ImmutableList.of("BANkb2MDZm9vATEA", "BANkb2MDYmxhATIA"), analysis.ids());
        assertEquals(ImmutableList.of("BANkb2MDZm9vATEA", "BANkb2MDYmxhATIA"), analysis.routingValues());
        assertFalse(analysis.whereClause().clusteredBy().isPresent());

        analysis = (SelectAnalysis)analyze(
            "select * from sys.shards where (schema_name='doc' and id = 1 and table_name = 'foo') " +
                    "or (schema_name='doc' and id = 2 and table_name = 'bla') or id = 1");
        assertEquals(ImmutableList.of(), analysis.ids());
        assertEquals(ImmutableList.of(), analysis.routingValues());
        assertFalse(analysis.whereClause().clusteredBy().isPresent());
    }

    @Test
    public void test1ColPrimaryKey() throws Exception {
        SelectAnalysis analysis = (SelectAnalysis)analyze("select name from sys.nodes where id='jalla'");
        assertEquals(ImmutableList.of("jalla"), analysis.ids());
        assertEquals(ImmutableList.of("jalla"), analysis.routingValues());

        analysis = (SelectAnalysis)analyze("select name from sys.nodes where 'jalla'=id");
        assertEquals(ImmutableList.of("jalla"), analysis.ids());

        analysis = (SelectAnalysis)analyze("select name from sys.nodes where id='jalla' and id='jalla'");
        assertEquals(ImmutableList.of("jalla"), analysis.ids());

        analysis = (SelectAnalysis)analyze("select name from sys.nodes where id='jalla' and (id='jalla' or 1=1)");
        assertEquals(ImmutableList.of("jalla"), analysis.ids());

        // a no match results in undefined key literals, since those are ambiguous
        analysis = (SelectAnalysis)analyze("select name from sys.nodes where id='jalla' and id='kelle'");
        assertEquals(ImmutableList.of(), analysis.ids());
        assertTrue(analysis.noMatch());

        analysis = (SelectAnalysis)analyze("select name from sys.nodes where id='jalla' or name = 'something'");
        assertEquals(ImmutableList.of(), analysis.ids());
        assertFalse(analysis.noMatch());

        analysis = (SelectAnalysis)analyze("select name from sys.nodes where name = 'something'");
        assertEquals(ImmutableList.of(), analysis.ids());
        assertFalse(analysis.noMatch());

    }

    @Test
    public void test3ColPrimaryKey() throws Exception {
        SelectAnalysis analysis = (SelectAnalysis)analyze("select id from sys.shards where id=1 and table_name='jalla' and schema_name='doc' and partition_ident=''");
        // base64 encoded versions of Streamable of ["doc","jalla","1"]
        assertEquals(ImmutableList.of("BANkb2MFamFsbGEBMQA="), analysis.ids());
        assertEquals(ImmutableList.of("BANkb2MFamFsbGEBMQA="), analysis.routingValues());
        assertFalse(analysis.noMatch());

        analysis = (SelectAnalysis)analyze("select id from sys.shards where id=1 and table_name='jalla' and id=1 and schema_name='doc' and partition_ident=''");
        // base64 encoded versions of Streamable of ["doc","jalla","1"]
        assertEquals(ImmutableList.of("BANkb2MFamFsbGEBMQA="), analysis.ids());
        assertEquals(ImmutableList.of("BANkb2MFamFsbGEBMQA="), analysis.routingValues());
        assertFalse(analysis.noMatch());


        analysis = (SelectAnalysis)analyze("select id from sys.shards where id=1");
        assertEquals(ImmutableList.of(), analysis.ids());
        assertFalse(analysis.noMatch());

        analysis = (SelectAnalysis)analyze("select id from sys.shards where id=1 and schema_name='doc' and table_name='jalla' and id=2 and partition_ident=''");
        assertEquals(ImmutableList.of(), analysis.ids());
        assertTrue(analysis.noMatch());
    }

    @Test
    public void test1ColPrimaryKeySetLiteralDiffMatches() throws Exception {
        SelectAnalysis analysis = (SelectAnalysis)analyze(
                "select name from sys.nodes where id in ('jalla', 'kelle') and id in ('jalla', 'something')");
        assertFalse(analysis.noMatch());
        assertEquals(1, analysis.ids().size());
        assertEquals("jalla", analysis.ids().get(0));
    }


    @Test
    public void test1ColPrimaryKeySetLiteral() throws Exception {
        SelectAnalysis analysis = (SelectAnalysis)analyze("select name from sys.nodes where id in ('jalla', 'kelle')");
        assertFalse(analysis.noMatch());
        assertEquals(2, analysis.ids().size());
        assertEquals(ImmutableList.of("jalla", "kelle"), analysis.ids());
    }

    @Test
    public void test3ColPrimaryKeySetLiteral() throws Exception {
        SelectAnalysis analysis = (SelectAnalysis)analyze("select id from sys.shards where id=1 and schema_name='doc' and table_name in ('jalla', 'kelle') and partition_ident=''");
        assertEquals(2, analysis.ids().size());
        // base64 encoded versions of Streamable of ["doc","jalla","1"] and ["doc","kelle","1"]
        assertEquals(ImmutableList.of("BANkb2MFamFsbGEBMQA=", "BANkb2MFa2VsbGUBMQA="), analysis.ids());
        assertEquals(ImmutableList.of("BANkb2MFamFsbGEBMQA=", "BANkb2MFa2VsbGUBMQA="), analysis.routingValues());
    }

    @Test
    public void testGranularityWithSingleAggregation() throws Exception {
        SelectAnalysis analysis = (SelectAnalysis)analyze("select count(*) from sys.nodes");
        assertThat(analysis.rowGranularity(), is(RowGranularity.NODE));
    }

    @Test
    public void testRewriteCountStringLiteral() {
        SelectAnalysis analysis = (SelectAnalysis)analyze("select count('id') from sys.nodes");
        List<Symbol> outputSymbols = analysis.outputSymbols;
        assertThat(outputSymbols.size(), is(1));
        assertThat(outputSymbols.get(0), instanceOf(Function.class));
        assertThat(((Function) outputSymbols.get(0)).arguments().size(), is(0));
    }

    @Test
    public void testRewriteCountNull() {
        SelectAnalysis analysis = (SelectAnalysis)analyze("select count(null) from sys.nodes");
        List<Symbol> outputSymbols = analysis.outputSymbols;
        assertThat(outputSymbols.size(), is(1));
        assertThat(outputSymbols.get(0), instanceOf(LongLiteral.class));
        assertThat(((LongLiteral) outputSymbols.get(0)).value(), is(0L));
    }

    @Test
    public void testWhereInSelect() throws Exception {
        SelectAnalysis analysis = (SelectAnalysis)analyze("select load from sys.nodes where load['1'] in (1.0, 2.0, 4.0, 8.0, 16.0)");

        Function whereClause = (Function)analysis.whereClause().query();
        assertEquals(InOperator.NAME, whereClause.info().ident().name());
        assertThat(whereClause.arguments().get(0), IsInstanceOf.instanceOf(Reference.class));
        assertThat(whereClause.arguments().get(1), IsInstanceOf.instanceOf(SetLiteral.class));
        SetLiteral setLiteral = (SetLiteral) whereClause.arguments().get(1);
        assertEquals(setLiteral.symbolType(), SymbolType.SET_LITERAL);
        assertEquals(setLiteral.valueType(), DataType.DOUBLE_SET);
    }

    @Test
    public void testWhereInSelectDifferentDataTypeList() throws Exception {
        SelectAnalysis analysis = (SelectAnalysis)analyze("select 'found' from users where 1 in (1.2, 2)");
        assertFalse(analysis.whereClause().hasQuery()); // already normalized from 1 in (1, 2) --> true
        assertFalse(analysis.whereClause().noMatch());
    }

    @Test
    public void testWhereInSelectDifferentDataTypeValue() throws Exception {
        SelectAnalysis analysis = (SelectAnalysis)analyze("select 'found' from users where 1.2 in (1, 2)");
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
        SelectAnalysis analysis = (SelectAnalysis) analyze("select count(distinct load['1']) from sys.nodes");

        assertTrue(analysis.hasAggregates());
        assertEquals(2, analysis.functions().size());

        Function collectionCount = getFunctionByName(CollectionCountFunction.NAME, analysis.functions());
        Function collectSet = getFunctionByName(CollectSetAggregation.NAME, analysis.functions());
        assertNotNull(collectionCount);
        assertNotNull(collectSet);

        List<Symbol> args = collectionCount.arguments();
        assertEquals(1, args.size());
        Function innerFunction = (Function) args.get(0);
        assertTrue(innerFunction.info().isAggregate());
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
        SelectAnalysis distinctAnalysis = (SelectAnalysis) analyze("select distinct count(*) from users");
        assertFalse(distinctAnalysis.hasGroupBy());
    }

    @Test
    public void testSelectGlobalDistinctRewriteAggregateionGroupBy() {
        SelectAnalysis distinctAnalysis = (SelectAnalysis) analyze("select distinct name, count(id) from users group by name");
        SelectAnalysis groupByAnalysis = (SelectAnalysis) analyze("select name, count(id) from users group by name");
        assertEquals(groupByAnalysis.groupBy(), distinctAnalysis.groupBy());
    }

    @Test
    public void testSelectGlobalDistinctRewrite() {
        SelectAnalysis distinctAnalysis = (SelectAnalysis) analyze("select distinct name from users");
        SelectAnalysis groupByAnalysis = (SelectAnalysis) analyze("select name from users group by name");
        assertEquals(groupByAnalysis.groupBy(), distinctAnalysis.groupBy());
    }

    @Test
    public void testSelectGlobalDistinctRewriteAllColumns() {
        SelectAnalysis distinctAnalysis = (SelectAnalysis) analyze("select distinct * from users");
        SelectAnalysis groupByAnalysis =
                (SelectAnalysis) analyze(
                        "select _version, awesome, details, friends, counters, id, name, other_id " +
                        "from users " +
                        "group by _version, awesome, details, friends, counters, id, name, other_id");
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
        SelectAnalysis analysis = (SelectAnalysis) analyze("select id from sys.nodes where load=?",
                new Object[]{map});
        Function whereClause = (Function)analysis.whereClause().query();
        assertThat(whereClause.arguments().get(1), instanceOf(ObjectLiteral.class));
        assertTrue(((ObjectLiteral) whereClause.arguments().get(1)).value().equals(map));
    }

    @Test
    public void testLikeInWhereQuery() {
        SelectAnalysis analysis = (SelectAnalysis)analyze("select * from sys.nodes where name like 'foo'");

        assertNotNull(analysis.whereClause());
        Function whereClause = (Function)analysis.whereClause().query();
        assertEquals(LikeOperator.NAME, whereClause.info().ident().name());
        ImmutableList<DataType> argumentTypes = ImmutableList.<DataType>of(DataType.STRING, DataType.STRING);
        assertEquals(argumentTypes, whereClause.info().ident().argumentTypes());

        assertThat(whereClause.arguments().get(0), IsInstanceOf.instanceOf(Reference.class));
        assertThat(whereClause.arguments().get(1), IsInstanceOf.instanceOf(StringLiteral.class));
        StringLiteral stringLiteral = (StringLiteral) whereClause.arguments().get(1);
        assertThat(stringLiteral.value(), is(new BytesRef(("foo"))));
    }

    @Test(expected = UnsupportedOperationException.class) // ESCAPE is not supported yet.
    public void testLikeEscapeInWhereQuery() {
        analyze("select * from sys.nodes where name like 'foo' escape 'o'");
    }

    @Test
    public void testLikeNoStringDataTypeInWhereQuery() {
        SelectAnalysis analysis = (SelectAnalysis)analyze("select * from sys.nodes where name like 1");

        // check if the implicit cast of the pattern worked
        ImmutableList<DataType> argumentTypes = ImmutableList.<DataType>of(DataType.STRING, DataType.STRING);
        Function whereClause = (Function)analysis.whereClause().query();
        assertEquals(argumentTypes, whereClause.info().ident().argumentTypes());
        assertThat(whereClause.arguments().get(1), IsInstanceOf.instanceOf(StringLiteral.class));
        StringLiteral stringLiteral = (StringLiteral) whereClause.arguments().get(1);
        assertThat(stringLiteral.value(), is(new BytesRef("1")));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testLikeReferenceInPatternInWhereQuery() {
        analyze("select * from sys.nodes where 1 like name");
    }

    @Test
    public void testLikeLongDataTypeInWhereQuery() {
        SelectAnalysis analysis = (SelectAnalysis)analyze("select * from sys.nodes where 1 like 2");

        // check if implicit cast worked of both, expression and pattern.
        Function function = (Function) analysis.functions().toArray()[0];
        assertEquals(LikeOperator.NAME, function.info().ident().name());
        ImmutableList<DataType> argumentTypes = ImmutableList.<DataType>of(DataType.STRING, DataType.STRING);
        assertEquals(argumentTypes, function.info().ident().argumentTypes());

        assertThat(function.arguments().get(0), IsInstanceOf.instanceOf(StringLiteral.class));
        assertThat(function.arguments().get(1), IsInstanceOf.instanceOf(StringLiteral.class));
        StringLiteral expressionLiteral = (StringLiteral) function.arguments().get(0);
        StringLiteral patternLiteral = (StringLiteral) function.arguments().get(1);
        assertThat(expressionLiteral.value(), is(new BytesRef("1")));
        assertThat(patternLiteral.value(), is(new BytesRef("2")));
    }

    @Test
    public void testIsNullInWhereQuery() {
        SelectAnalysis analysis = (SelectAnalysis)analyze("select * from sys.nodes where name is null");
        Function isNullFunction = (Function) analysis.functions().toArray()[0];

        assertThat(isNullFunction.info().ident().name(), is(IsNullPredicate.NAME));
        assertThat(isNullFunction.arguments().size(), is(1));
        assertThat(isNullFunction.arguments().get(0), IsInstanceOf.instanceOf(Reference.class));
        assertNotNull(analysis.whereClause());
    }

    @Test
    public void testNullIsNullInWhereQuery() {
        SelectAnalysis analysis = (SelectAnalysis)analyze("select * from sys.nodes where null is null");
        Function isNullFunction = (Function) analysis.functions().toArray()[0];
        assertThat(isNullFunction.arguments().get(0), IsInstanceOf.instanceOf(Null.class));
        assertFalse(analysis.whereClause().hasQuery());
        assertFalse(analysis.noMatch());
    }

    @Test
    public void testLongIsNullInWhereQuery() {
        SelectAnalysis analysis = (SelectAnalysis)analyze("select * from sys.nodes where 1 is null");
        Function isNullFunction = (Function) analysis.functions().toArray()[0];
        assertThat(isNullFunction.arguments().get(0), IsInstanceOf.instanceOf(LongLiteral.class));
        assertTrue(analysis.noMatch());
    }

    @Test
    public void testIsNullOnDynamicReference() {
        SelectAnalysis analysis = (SelectAnalysis)analyze("select \"_id\" from users where invalid is null");
        assertTrue(analysis.noMatch());
    }

    @Test
    public void testNotPredicate() {
        SelectAnalysis analysis = (SelectAnalysis)analyze("select * from users where name not like 'foo%'");
        assertThat(((Function)analysis.whereClause.query()).info().ident().name(), is(NotPredicate.NAME));
    }

    @Test
    public void testFilterByLiteralBoolean() throws Exception {
        SelectAnalysis analysis = (SelectAnalysis)analyze("select * from users where awesome=TRUE");
        assertThat(((Function) analysis.whereClause().query()).arguments().get(1).symbolType(), is(SymbolType.BOOLEAN_LITERAL));
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
        Analysis analysis = analyze("select count(*) from users limit 1 offset 1");
        assertTrue(analysis.hasNoResult());

        analysis = analyze("select count(*) from users limit 5 offset 1");
        assertTrue(analysis.hasNoResult());

        analysis = analyze("select count(*) from users limit 1");
        assertFalse(analysis.hasNoResult());

        analysis = analyze("select count(*) from users limit 0");
        assertTrue(analysis.hasNoResult());

        analysis = analyze("select name from users limit 0");
        assertTrue(analysis.hasNoResult());

        analysis = analyze("select name from users where false");
        assertTrue(analysis.hasNoResult());

        analysis = analyze("select name from users limit 10 offset 10");
        assertFalse(analysis.hasNoResult());
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
    public void testSelectFromPartitionedTable() throws Exception {
        String partition1 = new PartitionName("parted", Arrays.asList("1395874800000")).stringValue();
        String partition2 = new PartitionName("parted", Arrays.asList("1395961200000")).stringValue();
        String partition3 = new PartitionName("parted", new ArrayList<String>(){{add(null);}}).stringValue();

        SelectAnalysis analysis = (SelectAnalysis)analyze("select id, name from parted where date = 1395874800000");
        assertEquals(ImmutableList.of(partition1), analysis.whereClause().partitions());
        assertFalse(analysis.whereClause().hasQuery());
        assertFalse(analysis.noMatch());

        analysis = (SelectAnalysis)analyze("select id, name from parted where date >= 1395874800000");
        assertThat(analysis.whereClause().partitions(), containsInAnyOrder(partition1, partition2));
        assertFalse(analysis.whereClause().hasQuery());
        assertFalse(analysis.noMatch());

        analysis = (SelectAnalysis)analyze("select id, name from parted where date < 1395874800000");
        assertEquals(ImmutableList.of(), analysis.whereClause().partitions());
        assertTrue(analysis.noMatch());

        analysis = (SelectAnalysis)analyze("select id, name from parted where date = 1395874800000 and date = 1395961200000");
        assertEquals(ImmutableList.of(), analysis.whereClause().partitions());
        assertTrue(analysis.noMatch());

        analysis = (SelectAnalysis)analyze("select id, name from parted where date = 1395874800000 or date = 1395961200000");
        assertThat(analysis.whereClause().partitions(), containsInAnyOrder(partition1, partition2));
        assertFalse(analysis.whereClause().hasQuery());
        assertFalse(analysis.noMatch());

        analysis = (SelectAnalysis)analyze("select id, name from parted where date < 1395874800000 or date > 1395874800000");
        assertEquals(ImmutableList.of(partition2), analysis.whereClause().partitions());
        assertFalse(analysis.whereClause().hasQuery());
        assertFalse(analysis.noMatch());

        analysis = (SelectAnalysis)analyze("select id, name from parted where date in (1395874800000, 1395961200000)");
        assertThat(analysis.whereClause().partitions(), containsInAnyOrder(partition1, partition2));
        assertFalse(analysis.whereClause().hasQuery());
        assertFalse(analysis.noMatch());

        analysis = (SelectAnalysis)analyze("select id, name from parted where date in (1395874800000, 1395961200000) and id = 1");
        assertThat(analysis.whereClause().partitions(), containsInAnyOrder(partition1, partition2));
        assertTrue(analysis.whereClause().hasQuery());
        assertFalse(analysis.noMatch());

        analysis = (SelectAnalysis)analyze("select id, name from parted where date in (1395874800000) or date in (1395961200000)");
        assertThat(analysis.whereClause().partitions(), containsInAnyOrder(partition1, partition2));
        assertFalse(analysis.whereClause().hasQuery());
        assertFalse(analysis.noMatch());

        analysis = (SelectAnalysis)analyze("select id, name from parted where date = 1395961200000 and id = 1");
        assertEquals(ImmutableList.of(partition2), analysis.whereClause().partitions());
        assertTrue(analysis.whereClause().hasQuery());
        assertFalse(analysis.noMatch());

        analysis = (SelectAnalysis)analyze("select id, name from parted where (date =1395874800000 or date = 1395961200000) and id = 1");
        assertThat(analysis.whereClause().partitions(), containsInAnyOrder(partition1, partition2));
        assertTrue(analysis.whereClause().hasQuery());
        assertFalse(analysis.noMatch());

        analysis = (SelectAnalysis)analyze("select id, name from parted where date = 1395874800000 and id is null");
        assertEquals(ImmutableList.of(partition1), analysis.whereClause().partitions());
        assertTrue(analysis.whereClause().hasQuery());
        assertFalse(analysis.noMatch());

        analysis = (SelectAnalysis)analyze("select id, name from parted where date is null and id = 1");
        assertEquals(ImmutableList.of(partition3), analysis.whereClause().partitions());
        assertTrue(analysis.whereClause().hasQuery());
        assertFalse(analysis.noMatch());

    }

    @Test
    public void testSelectFromPartitionedTableUnsupported() throws Exception {
        // these queries won't work because we would have to execute 2 separate ESSearch tasks
        // and merge results which is not supported right now and maybe never will be
        try {
            analyze("select id, name from parted where date = 1395961200000 or id = 1");
            fail("Expected UnsupportedFeatureException");
        } catch (UnsupportedFeatureException e) {
        }

        try {
            analyze("select id, name from parted where id = 1 or date = 1395961200000");
            fail("Expected UnsupportedFeatureException");
        } catch (UnsupportedFeatureException e) {
        }
    }

    @Test
    public void testSelectPartitionedTableOrderBy() throws Exception {
        SelectAnalysis analysis = (SelectAnalysis)analyze(
                "select id, name from multi_parted order by id, abs(num), name");
        assertThat(analysis.sortSymbols().size(), is(3));
        assertThat(analysis.sortSymbols().get(0), Matchers.instanceOf(Reference.class));
        assertThat(analysis.sortSymbols().get(1), Matchers.instanceOf(Function.class));
        assertThat(analysis.sortSymbols().get(2), Matchers.instanceOf(Reference.class));
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
        analyze("select name from multi_parted order by obj['name']");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSelectOrderByPartitionedNestedColumnInFunction() throws Exception {
        analyze("select name from multi_parted order by format('abc %s', obj['name'])");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testArithmeticExpressions() throws Exception {
        analyze("select 1 + 1 from users");
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
        analyze("select * from users where 'George' = ANY_OF (name)");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testArrayCompareObjectArray() throws Exception {
        // TODO: remove this artificial limitation in general
        analyze("select * from users where ? = ANY_OF (friends)", new Object[]{
                new MapBuilder<String, Object>().put("id", 1L).map()
        });
    }

    @Test
    public void testArrayCompareAny() throws Exception {
        SelectAnalysis analysis = (SelectAnalysis) analyze("select * from users where 0 = ANY_OF (counters)");
        assertThat(analysis.whereClause().hasQuery(), is(true));

        FunctionInfo anyInfo = ((Function)analysis.whereClause().query()).info();
        assertThat(anyInfo.ident().name(), is("any_="));
        assertThat(anyInfo.ident().argumentTypes(), contains(DataType.LONG_ARRAY, DataType.LONG));

        analysis = (SelectAnalysis) analyze("select * from users where 0 = ANY_OF (counters)");
        assertThat(analysis.whereClause().hasQuery(), is(true));

        anyInfo = ((Function)analysis.whereClause().query()).info();
        assertThat(anyInfo.ident().name(), is("any_="));
        assertThat(anyInfo.ident().argumentTypes(), contains(DataType.LONG_ARRAY, DataType.LONG));
    }

    @Test
    public void testArrayCompareAnyNeq() throws Exception {
        SelectAnalysis analysis = (SelectAnalysis) analyze("select * from users where ? != ANY_OF (counters)",
                new Object[]{ 4.3F });
        assertThat(analysis.whereClause().hasQuery(), is(true));

        FunctionInfo anyInfo = ((Function)analysis.whereClause().query()).info();
        assertThat(anyInfo.ident().name(), is("any_<>"));
        assertThat(anyInfo.ident().argumentTypes(), contains(DataType.LONG_ARRAY, DataType.LONG));

    }

    @Test(expected = IllegalArgumentException.class)
    public void testArrayCompareAll() throws Exception {
        analyze("select * from users where 0 = ALL_OF (counters)");

    }
}
