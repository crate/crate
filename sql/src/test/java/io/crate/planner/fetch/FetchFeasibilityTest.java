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

package io.crate.planner.fetch;

import com.google.common.collect.ImmutableList;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.SqlExpressions;
import io.crate.testing.T3;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.Matchers.is;

public class FetchFeasibilityTest extends CrateUnitTest {

    private final SqlExpressions sqlExpressions = new SqlExpressions(T3.SOURCES, T3.TR_1);
    private final Function myFun = (Function) sqlExpressions.normalize(sqlExpressions.asSymbol("upper(a)"));
    private final Function bothFun = (Function) sqlExpressions.normalize(sqlExpressions.asSymbol("cast(a as integer) + x"));
    private final Function myOtherFun = (Function) sqlExpressions.normalize(sqlExpressions.asSymbol("lower(a)"));
    private final Reference a = T3.T1_INFO.getReference(new ColumnIdent("a"));
    private final Reference x = T3.T1_INFO.getReference(new ColumnIdent("x"));


    @Test
    public void testValidateWithoutFetchSymbols() throws Exception {
        List<Symbol> orderBySymbols = ImmutableList.of(myFun, a);
        List<Symbol> outputs = ImmutableList.of(myFun, a);

        assertThat(FetchFeasibility.isFetchFeasible(outputs, orderBySymbols), is(false));
    }

    @Test
    public void testValidateWithFetchSymbolsInFunction() throws Exception {
        List<Symbol> orderBySymbols = ImmutableList.of(myFun);
        List<Symbol> outputs = ImmutableList.of(myOtherFun);
        assertThat(FetchFeasibility.isFetchFeasible(outputs, orderBySymbols), is(true));
    }

    @Test
    public void testValidateWithFetchSymbolsUsedInOutPutFunction() throws Exception {
        List<Symbol> orderBySymbols = ImmutableList.of(a, x);
        List<Symbol> outputs = ImmutableList.of(bothFun);
        assertThat(FetchFeasibility.isFetchFeasible(outputs, orderBySymbols), is(false));
    }

    @Test
    public void testValidateNoQueryOrOrderBy() throws Exception {
        List<Symbol> orderBySymbols = ImmutableList.of();
        List<Symbol> outputs = ImmutableList.of(a, x);
        assertThat(FetchFeasibility.isFetchFeasible(outputs, orderBySymbols), is(true));

    }
}
