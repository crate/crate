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
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.operation.scalar.arithmetic.AbsFunction;
import io.crate.planner.symbol.Field;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataTypes;
import org.junit.Test;

import static io.crate.testing.TestingHelpers.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class FetchPushDownTest {

    private static final TableIdent TABLE_IDENT = new TableIdent("s", "t");
    private static final DocTableInfo TABLE_INFO = TestingTableInfo.builder(TABLE_IDENT, mock(Routing.class)).build();
    private static final DocTableRelation TABLE_REL = new DocTableRelation(TABLE_INFO);

    private static final Reference REF_SCORE = new Reference(
            DocSysColumns.forTable(TABLE_IDENT, DocSysColumns.SCORE));

    private static final Reference REF_I = new Reference(
            new ReferenceInfo(
                    new ReferenceIdent(new TableIdent("s", "t"), "i"),
                    RowGranularity.DOC,
                    DataTypes.INTEGER
            )
    );

    private static final Reference REF_A = new Reference(
            new ReferenceInfo(
                    new ReferenceIdent(new TableIdent("s", "t"), "a"),
                    RowGranularity.DOC,
                    DataTypes.STRING
            )
    );

    @Test
    public void testLimitIsPushedDown() throws Exception {
        QuerySpec qs = new QuerySpec();
        qs.outputs(Lists.<Symbol>newArrayList(REF_I, REF_A));
        qs.limit(10);
        qs.offset(100);

        FetchPushDown pd = new FetchPushDown(qs, TABLE_REL);
        QueriedDocTable sub = pd.pushDown();
        assertThat(sub.querySpec().limit(), is(110));
        assertThat(sub.querySpec().offset(), is(0));

        assertThat(qs.limit(), is(10));
        assertThat(qs.offset(), is(100));
    }

    @Test
    public void testWhereIsPushedDown() throws Exception {
        QuerySpec qs = new QuerySpec();
        qs.outputs(Lists.<Symbol>newArrayList(REF_I, REF_A));
        qs.where(WhereClause.NO_MATCH);

        FetchPushDown pd = new FetchPushDown(qs, TABLE_REL);
        QueriedDocTable sub = pd.pushDown();

        assertThat(sub.querySpec().where(), is(WhereClause.NO_MATCH));
        assertThat(qs.where(), is(WhereClause.MATCH_ALL));
    }


    @Test
    public void testPushDownWithoutOrder() throws Exception {
        QuerySpec qs = new QuerySpec();
        qs.outputs(Lists.<Symbol>newArrayList(REF_A, REF_I));
        FetchPushDown pd = new FetchPushDown(qs, TABLE_REL);
        QueriedDocTable sub = pd.pushDown();
        assertThat(qs.outputs(), hasSize(2));

        assertThat(((Field) ((FetchReference) qs.outputs().get(0)).docId()).index(), is(0));

        assertThat(qs.outputs(), contains(
                isFetchRef(0, "_doc['a']"),
                isFetchRef(0, "_doc['i']")
        ));

        assertThat(sub.querySpec().outputs(), hasSize(1));
        assertThat(sub.querySpec().outputs().get(0), isReference("_docid"));
    }

    @Test
    public void testPushDownWithOrder() throws Exception {
        QuerySpec qs = new QuerySpec();
        qs.outputs(Lists.<Symbol>newArrayList(REF_A, REF_I));
        qs.orderBy(new OrderBy(Lists.<Symbol>newArrayList(REF_I), new boolean[]{true}, new Boolean[]{false}));
        FetchPushDown pd = new FetchPushDown(qs, TABLE_REL);
        QueriedDocTable sub = pd.pushDown();
        assertThat(qs.outputs(), hasSize(2));

        assertThat(qs.outputs(), contains(
                isFetchRef(0, "_doc['a']"),
                isField(1)
        ));

        assertThat(sub.querySpec().outputs(), hasSize(2));
        assertThat(sub.querySpec().outputs(), contains(
                isReference("_docid"),
                isReference("i")));
    }

    private Function abs(Symbol symbol){
        return new Function(new AbsFunction(symbol.valueType()).info(),
                Lists.newArrayList(symbol));
    }

    @Test
    public void testPushDownWithNestedOrder() throws Exception {
        QuerySpec qs = new QuerySpec();

        qs.outputs(Lists.<Symbol>newArrayList(REF_A, REF_I));
        qs.orderBy(new OrderBy(Lists.<Symbol>newArrayList(abs(REF_I)), new boolean[]{true}, new Boolean[]{false}));
        FetchPushDown pd = new FetchPushDown(qs, TABLE_REL);
        QueriedDocTable sub = pd.pushDown();
        assertThat(qs.outputs(), hasSize(2));

        assertThat(qs.outputs(), contains(
                isFetchRef(0, "_doc['a']"),
                isFetchRef(0, "_doc['i']")
        ));

        assertThat(qs.orderBy().orderBySymbols().get(0), isField(1));

        assertThat(sub.querySpec().outputs(), hasSize(2));
        assertThat(sub.querySpec().outputs(), contains(
                isReference("_docid"),
                isFunction("abs")));
    }


    @Test
    public void testPushDownWithNestedOrderInOutput() throws Exception {
        QuerySpec qs = new QuerySpec();

        Function funcOfI = abs(REF_I);

        qs.outputs(Lists.newArrayList(REF_A, REF_I, funcOfI));
        qs.orderBy(new OrderBy(Lists.<Symbol>newArrayList(funcOfI), new boolean[]{true}, new Boolean[]{false}));
        FetchPushDown pd = new FetchPushDown(qs, TABLE_REL);
        QueriedDocTable sub = pd.pushDown();
        assertThat(qs.outputs(), hasSize(3));

        assertThat(qs.outputs(), contains(
                isFetchRef(0, "_doc['a']"),
                isFetchRef(0, "_doc['i']"),
                isField(1)
        ));

        assertThat(qs.orderBy().orderBySymbols().get(0), isField(1));

        assertThat(sub.querySpec().outputs(), hasSize(2));
        assertThat(sub.querySpec().outputs(), contains(
                isReference("_docid"),
                isFunction("abs")));

        assertThat(sub.querySpec().orderBy().orderBySymbols(), contains(
                isFunction("abs", isReference("i"))));
    }


    @Test
    public void testPushDownOrderRefUsedInFunction() throws Exception {
        QuerySpec qs = new QuerySpec();

        Function funcOfI = abs(REF_I);

        qs.outputs(Lists.newArrayList(REF_A, REF_I, funcOfI));
        qs.orderBy(new OrderBy(
                Lists.<Symbol>newArrayList(REF_I), new boolean[]{true}, new Boolean[]{false}));
        FetchPushDown pd = new FetchPushDown(qs, TABLE_REL);
        QueriedDocTable sub = pd.pushDown();
        assertThat(qs.outputs(), hasSize(3));

        assertThat(qs.outputs(), contains(
                isFetchRef(0, "_doc['a']"),
                isField(1),
                isFunction("abs")
        ));

        assertThat(qs.orderBy().orderBySymbols().get(0), isField(1));

        assertThat(sub.querySpec().outputs(), hasSize(2));
        assertThat(sub.querySpec().outputs(), contains(
                isReference("_docid"),
                isReference("i")));

        // check that the reference in the abs function is a field
        assertThat(((Function) qs.outputs().get(2)).arguments().get(0), isField(1));
    }


    @Test
    public void testNoPushDownWithOrder() throws Exception {
        QuerySpec qs = new QuerySpec();
        qs.outputs(Lists.<Symbol>newArrayList(REF_I));
        qs.orderBy(new OrderBy(ImmutableList.<Symbol>of(REF_I), new boolean[]{true}, new Boolean[]{false}));

        FetchPushDown pd = new FetchPushDown(qs, TABLE_REL);
        assertNull(pd.pushDown());
        assertThat(qs.outputs(), contains(isReference("i")));
    }

    @Test
    public void testScoreDoesNotRequireFetch() throws Exception {
        QuerySpec qs = new QuerySpec();
        qs.outputs(Lists.<Symbol>newArrayList(REF_I, REF_SCORE));
        qs.orderBy(new OrderBy(ImmutableList.<Symbol>of(REF_I), new boolean[]{true}, new Boolean[]{false}));
        FetchPushDown pd = new FetchPushDown(qs, TABLE_REL);
        assertNull(pd.pushDown());
    }

    @Test
    public void testScoreGetsPushedDown() throws Exception {
        QuerySpec qs = new QuerySpec();
        qs.outputs(Lists.<Symbol>newArrayList(REF_A, REF_I, REF_SCORE));
        qs.orderBy(new OrderBy(Lists.<Symbol>newArrayList(REF_I), new boolean[]{true}, new Boolean[]{false}));

        FetchPushDown pd = new FetchPushDown(qs, TABLE_REL);
        QueriedDocTable sub = pd.pushDown();
        assertThat(sub, notNullValue());

        assertThat(sub.querySpec().outputs(), contains(
                isReference("_docid"),
                isReference("i"),
                isReference("_score")));

        assertThat(qs.outputs(), contains(
                isFetchRef(0, "_doc['a']"),
                isField(1),
                isField(2)
        ));
    }
}