/*
 * Licensed to Crate.IO GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.planner.fetch;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.SelectAnalyzedStatement;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.Routing;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.operation.scalar.arithmetic.AbsFunction;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.T3;
import io.crate.types.DataTypes;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isReference;
import static io.crate.testing.TestingHelpers.isSQL;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class FetchRewriterTest extends CrateDummyClusterServiceUnitTest {

    private static final TableIdent TABLE_IDENT = new TableIdent("s", "t");
    private static final DocTableInfo TABLE_INFO = TestingTableInfo.builder(TABLE_IDENT, mock(Routing.class)).build();
    private static final DocTableRelation TABLE_REL = new DocTableRelation(TABLE_INFO);

    private static final Reference REF_SCORE = DocSysColumns.forTable(TABLE_IDENT, DocSysColumns.SCORE);

    private static final Reference REF_I = new Reference(
        new ReferenceIdent(new TableIdent("s", "t"), "i"),
        RowGranularity.DOC,
        DataTypes.INTEGER);

    private static final Reference REF_A = new Reference(
        new ReferenceIdent(new TableIdent("s", "t"), "a"),
        RowGranularity.DOC,
        DataTypes.STRING);

    private SQLExecutor e;

    @Before
    public void setupExecutor() throws Exception {
        e = SQLExecutor.builder(clusterService).addDocTable(T3.T1_INFO).build();
    }

    @Test
    public void testRewriteWithoutOrderBy() throws Exception {
        QuerySpec qs = new QuerySpec();
        qs.outputs(Lists.newArrayList(REF_A, REF_I));
        QueriedDocTable table = new QueriedDocTable(TABLE_REL, qs);
        FetchRewriter.rewrite(table);
        assertThat(table.querySpec(), isSQL("SELECT s.t._fetchid"));
    }

    @Test
    public void testRewriteWithOrderBy() throws Exception {
        QuerySpec qs = new QuerySpec();
        qs.outputs(Lists.newArrayList(REF_A, REF_I));
        qs.orderBy(new OrderBy(Lists.newArrayList(REF_I), new boolean[]{true}, new Boolean[]{false}));
        QueriedDocTable table = new QueriedDocTable(TABLE_REL, qs);

        FetchRewriter.rewrite(table);

        assertThat(table.querySpec(), isSQL("SELECT s.t._fetchid, s.t.i ORDER BY s.t.i DESC NULLS LAST"));
    }

    @Test
    public void testRewriteWithFunctionInOrderBy() throws Exception {
        QuerySpec qs = new QuerySpec();
        qs.outputs(Lists.newArrayList(REF_A, REF_I));
        qs.orderBy(new OrderBy(Lists.newArrayList(abs(REF_I)), new boolean[]{true}, new Boolean[]{false}));
        QueriedDocTable table = new QueriedDocTable(TABLE_REL, qs);

        FetchRewriter.rewrite(table);
        assertThat(table.querySpec(), isSQL("SELECT s.t._fetchid, abs(s.t.i) ORDER BY abs(s.t.i) DESC NULLS LAST"));
    }

    @Test
    public void testPushDownWithNestedOrderInOutput() throws Exception {
        QuerySpec qs = new QuerySpec();
        Function funcOfI = abs(REF_I);
        qs.outputs(Lists.newArrayList(REF_A, REF_I, funcOfI));
        qs.orderBy(new OrderBy(Lists.newArrayList(funcOfI), new boolean[]{true}, new Boolean[]{false}));
        QueriedDocTable table = new QueriedDocTable(TABLE_REL, qs);

        FetchRewriter.rewrite(table);

        assertThat(table.querySpec(), isSQL("SELECT s.t._fetchid, abs(s.t.i) ORDER BY abs(s.t.i) DESC NULLS LAST"));
    }

    @Test
    public void testPushDownOrderRefUsedInFunction() throws Exception {
        QuerySpec qs = new QuerySpec();
        Function funcOfI = abs(REF_I);
        qs.outputs(Lists.newArrayList(REF_A, REF_I, funcOfI));
        qs.orderBy(new OrderBy(
            Lists.newArrayList(REF_I), new boolean[]{true}, new Boolean[]{false}));
        QueriedDocTable table = new QueriedDocTable(TABLE_REL, qs);

        FetchRewriter.rewrite(table);

        assertThat(table.querySpec(), isSQL("SELECT s.t._fetchid, s.t.i ORDER BY s.t.i DESC NULLS LAST"));
    }

    @Test
    public void testNoPushDownWithOrder() throws Exception {
        QuerySpec qs = new QuerySpec();
        qs.outputs(Lists.newArrayList(REF_I));
        qs.orderBy(new OrderBy(ImmutableList.of(REF_I), new boolean[]{true}, new Boolean[]{false}));

        assertThat(FetchRewriter.isFetchFeasible(qs), is(false));
    }

    @Test
    public void testScoreDoesNotRequireFetch() throws Exception {
        QuerySpec qs = new QuerySpec();
        qs.outputs(Lists.newArrayList(REF_I, REF_SCORE));
        qs.orderBy(new OrderBy(ImmutableList.of(REF_I), new boolean[]{true}, new Boolean[]{false}));

        assertThat(FetchRewriter.isFetchFeasible(qs), is(false));
    }

    @Test
    public void testScoreGetsPushedDown() throws Exception {
        QuerySpec qs = new QuerySpec();
        qs.outputs(Lists.newArrayList(REF_A, REF_I, REF_SCORE));
        qs.orderBy(new OrderBy(Lists.newArrayList(REF_I), new boolean[]{true}, new Boolean[]{false}));
        QueriedDocTable table = new QueriedDocTable(TABLE_REL, qs);

        FetchRewriter.rewrite(table);

        assertThat(table.querySpec(), isSQL("SELECT s.t._fetchid, s.t.i, s.t._score ORDER BY s.t.i DESC NULLS LAST"));
    }

    @Test
    public void testQuerySymbolsAreNotFetchedIfUsedWithinSelectListFunction() throws Exception {
        SelectAnalyzedStatement stmt = e.analyze("select x + x, a from t1 order by x");
        QueriedDocTable docTable = (QueriedDocTable) stmt.relation();
        FetchRewriter.FetchDescription fetchDescription = FetchRewriter.rewrite(docTable);

        assertThat(fetchDescription.postFetchOutputs, contains(isFunction("add"), isReference("_doc['a']")));
        assertThat(((Function) fetchDescription.postFetchOutputs.get(0)).arguments().get(0), isReference("x"));

        List<Symbol> fetchOutputs = FetchRewriter.generateFetchOutputs(fetchDescription);
        assertThat(fetchOutputs, isSQL("add(INPUT(1), INPUT(1)), FETCH(INPUT(0), doc.t1._doc['a'])"));
    }

    @Test
    public void testPreFetchOutputCanContainFunctions() throws Exception {
        SelectAnalyzedStatement stmt = e.analyze("select x + x, a from t1 order by 1");
        QueriedDocTable docTable = (QueriedDocTable) stmt.relation();
        FetchRewriter.FetchDescription fetchDescription = FetchRewriter.rewrite(docTable);

        assertThat(fetchDescription.preFetchOutputs(), contains(isReference("_fetchid"), isFunction("add")));
        assertThat(fetchDescription.postFetchOutputs, contains(isFunction("add"), isReference("_doc['a']")));

        List<Symbol> fetchOutputs = FetchRewriter.generateFetchOutputs(fetchDescription);
        assertThat(fetchOutputs, isSQL("INPUT(1), FETCH(INPUT(0), doc.t1._doc['a'])"));
    }

    private static Function abs(Symbol symbol) {
        return new Function(new AbsFunction(symbol.valueType()).info(), Lists.newArrayList(symbol));
    }
}
