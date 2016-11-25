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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.operation.scalar.arithmetic.AbsFunction;
import io.crate.types.DataTypes;
import org.junit.Test;

import static io.crate.testing.TestingHelpers.isSQL;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class FetchPushDownTest {

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

    @Test
    public void testLimitIsPushedDown() throws Exception {
        QuerySpec qs = new QuerySpec();
        qs.outputs(Lists.<Symbol>newArrayList(REF_I, REF_A));
        qs.limit(Optional.of(Literal.of(10)));
        qs.offset(Optional.of(Literal.of(100)));

        FetchPushDown pd = new FetchPushDown(qs, TABLE_REL, (byte) 0);
        QueriedDocTable sub = pd.pushDown();
        assertThat(sub.querySpec().limit().get(), is(Literal.of(10)));
        assertThat(sub.querySpec().offset().get(), is(Literal.of(100)));

        assertThat(qs.limit().get(), is(Literal.of(10)));
        assertThat(qs.offset().get(), is(Literal.of(100)));
    }

    @Test
    public void testWhereIsPushedDown() throws Exception {
        QuerySpec qs = new QuerySpec();
        qs.outputs(Lists.<Symbol>newArrayList(REF_I, REF_A));
        qs.where(WhereClause.NO_MATCH);

        FetchPushDown pd = new FetchPushDown(qs, TABLE_REL, (byte) 0);
        QueriedDocTable sub = pd.pushDown();

        assertThat(sub.querySpec().where(), is(WhereClause.NO_MATCH));
        assertThat(qs.where(), is(WhereClause.MATCH_ALL));
    }


    @Test
    public void testPushDownWithoutOrder() throws Exception {
        QuerySpec qs = new QuerySpec();
        qs.outputs(Lists.<Symbol>newArrayList(REF_A, REF_I));
        FetchPushDown pd = new FetchPushDown(qs, TABLE_REL, (byte) 0);
        QueriedDocTable sub = pd.pushDown();
        assertThat(qs, isSQL("SELECT FETCH(INPUT(0), s.t._doc['a']), FETCH(INPUT(0), s.t._doc['i'])"));
        assertThat(sub.querySpec(), isSQL("SELECT s.t._fetchid"));
    }

    @Test
    public void testPushDownWithOrder() throws Exception {
        QuerySpec qs = new QuerySpec();
        qs.outputs(Lists.<Symbol>newArrayList(REF_A, REF_I));
        qs.orderBy(new OrderBy(Lists.<Symbol>newArrayList(REF_I), new boolean[]{true}, new Boolean[]{false}));
        FetchPushDown pd = new FetchPushDown(qs, TABLE_REL, (byte) 0);
        QueriedDocTable sub = pd.pushDown();
        assertThat(qs, isSQL("SELECT FETCH(INPUT(0), s.t._doc['a']), INPUT(1) ORDER BY INPUT(1) DESC NULLS LAST"));
        assertThat(sub.querySpec(), isSQL("SELECT s.t._fetchid, s.t.i ORDER BY s.t.i DESC NULLS LAST"));
    }

    private Function abs(Symbol symbol) {
        return new Function(new AbsFunction(symbol.valueType()).info(),
            Lists.newArrayList(symbol));
    }

    @Test
    public void testPushDownWithNestedOrder() throws Exception {
        QuerySpec qs = new QuerySpec();

        qs.outputs(Lists.<Symbol>newArrayList(REF_A, REF_I));
        qs.orderBy(new OrderBy(Lists.<Symbol>newArrayList(abs(REF_I)), new boolean[]{true}, new Boolean[]{false}));
        FetchPushDown pd = new FetchPushDown(qs, TABLE_REL, (byte) 0);
        QueriedDocTable sub = pd.pushDown();
        assertThat(qs, isSQL("SELECT FETCH(INPUT(0), s.t._doc['a']), FETCH(INPUT(0), s.t._doc['i']) ORDER BY INPUT(1) DESC NULLS LAST"));
        assertThat(sub.querySpec(), isSQL("SELECT s.t._fetchid, abs(s.t.i) ORDER BY abs(s.t.i) DESC NULLS LAST"));
    }


    @Test
    public void testPushDownWithNestedOrderInOutput() throws Exception {
        QuerySpec qs = new QuerySpec();

        Function funcOfI = abs(REF_I);

        qs.outputs(Lists.newArrayList(REF_A, REF_I, funcOfI));
        qs.orderBy(new OrderBy(Lists.<Symbol>newArrayList(funcOfI), new boolean[]{true}, new Boolean[]{false}));
        FetchPushDown pd = new FetchPushDown(qs, TABLE_REL, (byte) 0);
        QueriedDocTable sub = pd.pushDown();
        assertThat(qs, isSQL("SELECT FETCH(INPUT(0), s.t._doc['a']), FETCH(INPUT(0), s.t._doc['i']), INPUT(1) ORDER BY INPUT(1) DESC NULLS LAST"));
        assertThat(sub.querySpec(), isSQL("SELECT s.t._fetchid, abs(s.t.i) ORDER BY abs(s.t.i) DESC NULLS LAST"));
    }


    @Test
    public void testPushDownOrderRefUsedInFunction() throws Exception {
        QuerySpec qs = new QuerySpec();
        Function funcOfI = abs(REF_I);
        qs.outputs(Lists.newArrayList(REF_A, REF_I, funcOfI));
        qs.orderBy(new OrderBy(
            Lists.<Symbol>newArrayList(REF_I), new boolean[]{true}, new Boolean[]{false}));
        FetchPushDown pd = new FetchPushDown(qs, TABLE_REL, (byte) 0);
        QueriedDocTable sub = pd.pushDown();
        assertThat(qs, isSQL("SELECT FETCH(INPUT(0), s.t._doc['a']), INPUT(1), abs(INPUT(1)) ORDER BY INPUT(1) DESC NULLS LAST"));
        assertThat(sub.querySpec(), isSQL("SELECT s.t._fetchid, s.t.i ORDER BY s.t.i DESC NULLS LAST"));
    }


    @Test
    public void testNoPushDownWithOrder() throws Exception {
        QuerySpec qs = new QuerySpec();
        qs.outputs(Lists.<Symbol>newArrayList(REF_I));
        qs.orderBy(new OrderBy(ImmutableList.<Symbol>of(REF_I), new boolean[]{true}, new Boolean[]{false}));
        FetchPushDown pd = new FetchPushDown(qs, TABLE_REL, (byte) 0);
        assertNull(pd.pushDown());
        assertThat(qs, isSQL("SELECT s.t.i ORDER BY s.t.i DESC NULLS LAST"));
    }

    @Test
    public void testScoreDoesNotRequireFetch() throws Exception {
        QuerySpec qs = new QuerySpec();
        qs.outputs(Lists.<Symbol>newArrayList(REF_I, REF_SCORE));
        qs.orderBy(new OrderBy(ImmutableList.<Symbol>of(REF_I), new boolean[]{true}, new Boolean[]{false}));
        FetchPushDown pd = new FetchPushDown(qs, TABLE_REL, (byte) 0);
        assertNull(pd.pushDown());
    }

    @Test
    public void testScoreGetsPushedDown() throws Exception {
        QuerySpec qs = new QuerySpec();
        qs.outputs(Lists.<Symbol>newArrayList(REF_A, REF_I, REF_SCORE));
        qs.orderBy(new OrderBy(Lists.<Symbol>newArrayList(REF_I), new boolean[]{true}, new Boolean[]{false}));
        FetchPushDown pd = new FetchPushDown(qs, TABLE_REL, (byte) 0);
        QueriedDocTable sub = pd.pushDown();
        assertThat(qs, isSQL("SELECT FETCH(INPUT(0), s.t._doc['a']), INPUT(1), INPUT(2) ORDER BY INPUT(1) DESC NULLS LAST"));
        assertThat(sub.querySpec(), isSQL("SELECT s.t._fetchid, s.t.i, s.t._score ORDER BY s.t.i DESC NULLS LAST"));
    }
}
