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

package io.crate.planner.operators;

import io.crate.analyze.symbol.Symbol;
import io.crate.testing.SqlExpressions;
import io.crate.testing.T3;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;

public class CollectTest {


    @Test
    public void testGetUnusedColsWithNoUnused() throws Exception {
        SqlExpressions sqlExpressions = new SqlExpressions(T3.SOURCES);
        Symbol x = sqlExpressions.asSymbol("x");
        Symbol xAdd1 = sqlExpressions.asSymbol("x + 1");

        List<Symbol> toCollect = Arrays.asList(x, xAdd1);
        Set<Symbol> used = Collections.singleton(xAdd1);

        assertThat(
            Collect.getUnusedColumns(toCollect, used),
            Matchers.emptyIterable()
        );
    }
}
