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
import io.crate.analyze.OrderBy;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.WhereClause;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.TableIdent;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataTypes;
import org.junit.Test;

import static io.crate.testing.TestingHelpers.isReference;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

public class FetchPushDownTest {

    private static final TableIdent TABLE_IDENT = new TableIdent("s", "t");
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
        qs.outputs(ImmutableList.<Symbol>of(REF_I, REF_A));
        qs.limit(10);
        qs.offset(100);

        QuerySpec sub = FetchPushDown.pushDown(qs, TABLE_IDENT);
        assertThat(sub.limit(), is(110));
        assertThat(sub.offset(), is(0));

        assertThat(qs.limit(), is(10));
        assertThat(qs.offset(), is(100));
    }

    @Test
    public void testWhereIsPushedDown() throws Exception {
        QuerySpec qs = new QuerySpec();
        qs.outputs(ImmutableList.<Symbol>of(REF_I, REF_A));
        qs.where(WhereClause.NO_MATCH);

        QuerySpec sub = FetchPushDown.pushDown(qs, TABLE_IDENT);

        assertThat(sub.where(), is(WhereClause.NO_MATCH));
        assertThat(qs.where(), is(WhereClause.MATCH_ALL));
    }


    @Test
    public void testPushDownWithoutOrder() throws Exception {
        QuerySpec qs = new QuerySpec();
        qs.outputs(ImmutableList.<Symbol>of(REF_A, REF_I));
        QuerySpec sub = FetchPushDown.pushDown(qs, TABLE_IDENT);
        assertThat(qs.outputs(), hasSize(2));

        assertThat(qs.outputs().get(0), isReference("a"));
        assertThat(qs.outputs().get(1), isReference("i"));

        assertThat(sub.outputs(), hasSize(1));
        assertThat(sub.outputs().get(0), isReference("_docid"));
    }

    @Test
    public void testPushDownWithOrder() throws Exception {
        QuerySpec qs = new QuerySpec();
        qs.outputs(ImmutableList.<Symbol>of(REF_A, REF_I));
        qs.orderBy(new OrderBy(ImmutableList.<Symbol>of(REF_I), new boolean[]{true}, new Boolean[]{false}));
        QuerySpec sub = FetchPushDown.pushDown(qs, TABLE_IDENT);
        assertThat(qs.outputs(), hasSize(2));

        assertThat(qs.outputs().get(0), isReference("a"));
        assertThat(qs.outputs().get(1), isReference("i"));

        assertThat(sub.outputs(), hasSize(2));
        assertThat(sub.outputs().get(0), isReference("_docid"));
        assertThat(sub.outputs().get(1), isReference("i"));
    }

    @Test
    public void testNoPushDownWithOrder() throws Exception {
        QuerySpec qs = new QuerySpec();
        qs.outputs(ImmutableList.<Symbol>of(REF_I));
        qs.orderBy(new OrderBy(ImmutableList.<Symbol>of(REF_I), new boolean[]{true}, new Boolean[]{false}));

        assertNull(FetchPushDown.pushDown(qs, TABLE_IDENT));
        assertThat(qs.outputs(), contains(isReference("i")));
    }
}