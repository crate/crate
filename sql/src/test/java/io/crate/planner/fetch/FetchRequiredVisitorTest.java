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
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataTypes;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.hamcrest.Matchers.is;

public class FetchRequiredVisitorTest extends CrateUnitTest {

    private final Reference x = TestingHelpers.createReference("x", DataTypes.STRING);
    private final Reference y = TestingHelpers.createReference("y", DataTypes.FLOAT);
    private final Function myFun = TestingHelpers.createFunction("my_fun", DataTypes.STRING, x);
    private final Function bothFun = TestingHelpers.createFunction("both", DataTypes.STRING, x, y);
    private final Function myOtherFun = TestingHelpers.createFunction("my_other_fun", DataTypes.STRING, x);


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
        List<Symbol> orderBySymbols = ImmutableList.of(myFun, y);
        List<Symbol> outputs = ImmutableList.of(myFun, y);

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

        List<Symbol> orderBySymbols = ImmutableList.<Symbol>of(x, y);
        List<Symbol> outputs = ImmutableList.<Symbol>of(bothFun);
        FetchRequiredVisitor.Context context = context(orderBySymbols);
        assertThat(FetchRequiredVisitor.INSTANCE.process(outputs, context), is(false));
    }

    @Test
    public void testValidateNoQueryOrOrderBy() throws Exception {
        List<Symbol> orderBySymbols = ImmutableList.of();
        List<Symbol> outputs = ImmutableList.<Symbol>of(x, y);
        FetchRequiredVisitor.Context context = context(orderBySymbols);
        assertThat(FetchRequiredVisitor.INSTANCE.process(outputs, context), is(true));

    }
}
