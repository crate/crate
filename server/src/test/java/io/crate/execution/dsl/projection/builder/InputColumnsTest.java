/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.execution.dsl.projection.builder;

import static io.crate.testing.Asserts.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.RelationName;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SqlExpressions;
import io.crate.testing.T3;

public class InputColumnsTest extends CrateDummyClusterServiceUnitTest {

    private SqlExpressions sqlExpressions;

    @Before
    public void prepare() throws Exception {
        Map<RelationName, AnalyzedRelation> sources = T3.sources(List.of(T3.T1), clusterService);

        DocTableRelation tr1 = (DocTableRelation) sources.get(T3.T1);
        sqlExpressions = new SqlExpressions(sources, tr1);
    }

    @Test
    public void testNonDeterministicFunctionsReplacement() throws Exception {
        Function fn1 = (Function) sqlExpressions.asSymbol("random()");
        Function fn2 = (Function) sqlExpressions.asSymbol("random()");

        List<Symbol> inputSymbols = Arrays.<Symbol>asList(
            Literal.BOOLEAN_FALSE,
            sqlExpressions.asSymbol("upper(a)"),
            fn1,
            fn2
        );

        Function newSameFn = (Function) sqlExpressions.asSymbol("random()");
        Function newDifferentFn = (Function) sqlExpressions.asSymbol("random()");
        InputColumns.SourceSymbols sourceSymbols = new InputColumns.SourceSymbols(inputSymbols);

        Symbol replaced1 = InputColumns.create(fn1, sourceSymbols);
        assertThat(replaced1).isExactlyInstanceOf(InputColumn.class);
        assertThat(((InputColumn) replaced1).index(), is(2));

        Symbol replaced2 = InputColumns.create(fn2, sourceSymbols);
        assertThat(replaced2).isExactlyInstanceOf(InputColumn.class);
        assertThat(((InputColumn) replaced2).index(), is(3));

        Symbol replaced3 = InputColumns.create(newSameFn, sourceSymbols);
        assertThat(replaced3, is(equalTo(newSameFn))); // not replaced

        Symbol replaced4 = InputColumns.create(newDifferentFn, sourceSymbols);
        assertThat(replaced4, is(equalTo(newDifferentFn))); // not replaced
    }
}
