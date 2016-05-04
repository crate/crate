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
import io.crate.analyze.OrderBy;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Reference;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.SqlExpressions;
import io.crate.testing.T3;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.hamcrest.Matchers.is;

public class FetchRequiredVisitorTest extends CrateUnitTest {

    private final SqlExpressions sqlExpressions = new SqlExpressions(T3.SOURCES, T3.TR_1);
    private final Function myFun = (Function) sqlExpressions.normalize(sqlExpressions.asSymbol("upper(a)"));
    private final Function bothFun = (Function) sqlExpressions.normalize(sqlExpressions.asSymbol("cast(a as integer) + x"));
    private final Function myOtherFun = (Function) sqlExpressions.normalize(sqlExpressions.asSymbol("lower(a)"));
    private final Reference a = new Reference(T3.T1_INFO.getReferenceInfo(new ColumnIdent("a")));
    private final Reference x = new Reference(T3.T1_INFO.getReferenceInfo(new ColumnIdent("x")));


    private FetchRequiredVisitor.Context context(List<Symbol> orderBySymbols) {
        if (!orderBySymbols.isEmpty()){
            boolean[] reverseFlags = new boolean[orderBySymbols.size()];
            Arrays.fill(reverseFlags, true);
            Boolean[] nullsFirst = new Boolean[orderBySymbols.size()];
            Arrays.fill(nullsFirst, Boolean.FALSE);
            OrderBy orderBy = new OrderBy(orderBySymbols, reverseFlags, nullsFirst);
            return new FetchRequiredVisitor.Context(new HashSet<>(orderBy.orderBySymbols()));
        }
        return new FetchRequiredVisitor.Context();
    }

    @Test
    public void testValidateWithoutFetchSymbols() throws Exception {
        List<Symbol> orderBySymbols = ImmutableList.of(myFun, a);
        List<Symbol> outputs = ImmutableList.of(myFun, a);

        FetchRequiredVisitor.Context context = context(orderBySymbols);
        assertThat(FetchRequiredVisitor.INSTANCE.process(outputs, context), is(false));
    }

    @Test
    public void testValidateWithFetchSymbolsInFunction() throws Exception {

        List<Symbol> orderBySymbols = ImmutableList.<Symbol>of(myFun);
        List<Symbol> outputs = ImmutableList.<Symbol>of(myOtherFun);
        FetchRequiredVisitor.Context context = context(orderBySymbols);
        assertThat(FetchRequiredVisitor.INSTANCE.process(outputs, context), is(true));
    }

    @Test
    public void testValidateWithFetchSymbolsUsedInOutPutFunction() throws Exception {

        List<Symbol> orderBySymbols = ImmutableList.<Symbol>of(a, x);
        List<Symbol> outputs = ImmutableList.<Symbol>of(bothFun);
        FetchRequiredVisitor.Context context = context(orderBySymbols);
        assertThat(FetchRequiredVisitor.INSTANCE.process(outputs, context), is(false));
    }

    @Test
    public void testValidateNoQueryOrOrderBy() throws Exception {
        List<Symbol> orderBySymbols = ImmutableList.of();
        List<Symbol> outputs = ImmutableList.<Symbol>of(a, x);
        FetchRequiredVisitor.Context context = context(orderBySymbols);
        assertThat(FetchRequiredVisitor.INSTANCE.process(outputs, context), is(true));

    }
}
