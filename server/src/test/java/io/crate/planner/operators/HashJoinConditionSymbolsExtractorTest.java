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

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.expression.scalar.arithmetic.ArithmeticFunctions;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.RelationName;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SqlExpressions;
import io.crate.testing.T3;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isLiteral;
import static io.crate.testing.SymbolMatchers.isReference;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class HashJoinConditionSymbolsExtractorTest extends CrateDummyClusterServiceUnitTest {

    private SqlExpressions sqlExpressions;
    private AnalyzedRelation tr1;
    private AnalyzedRelation tr2;

    @Before
    public void prepare() throws Exception {
        Map<RelationName, AnalyzedRelation> sources = T3.sources(clusterService);
        sqlExpressions = new SqlExpressions(sources);
        tr1 = sources.get(T3.T1);
        tr2 = sources.get(T3.T2);
    }

    @Test
    public void testExtractFromTopEqCondition() {
        Symbol joinCondition = sqlExpressions.asSymbol("t1.x = t2.y");
        Map<RelationName, List<Symbol>> symbolsPerRelation = HashJoinConditionSymbolsExtractor.extract(joinCondition);
        assertThat(symbolsPerRelation.get(tr1.relationName()), contains(isReference("x")));
        assertThat(symbolsPerRelation.get(tr2.relationName()), contains(isReference("y")));
    }

    @Test
    public void testExtractFromNestedEqCondition() {
        Symbol joinCondition = sqlExpressions.asSymbol("t1.x > t2.y and t1.a = t2.b and not(t1.i = t2.i)");
        Map<RelationName, List<Symbol>> symbolsPerRelation = HashJoinConditionSymbolsExtractor.extract(joinCondition);
        assertThat(symbolsPerRelation.get(tr1.relationName()), contains(isReference("a")));
        assertThat(symbolsPerRelation.get(tr2.relationName()), contains(isReference("b")));
    }

    @Test
    public void testExtractSymbolsWithDuplicates() {
        Symbol joinCondition = sqlExpressions.asSymbol(
            "t1.a = t2.b and t1.i + 1::int = t2.i and t2.y = t1.i + 1::int");
        Map<RelationName, List<Symbol>> symbolsPerRelation = HashJoinConditionSymbolsExtractor.extract(joinCondition);
        assertThat(symbolsPerRelation.get(tr1.relationName()), containsInAnyOrder(
            isReference("a"),
            isFunction(ArithmeticFunctions.Names.ADD, isReference("i"), isLiteral(1))));
        assertThat(symbolsPerRelation.get(tr2.relationName()), containsInAnyOrder(isReference("b"), isReference("i")));
    }

    @Test
    public void testExtractRelationsOfFunctionsWithLiterals() {
        Symbol joinCondition = sqlExpressions.asSymbol(
            "t1.a = t2.b and t1.i + 1::int = t2.i and t2.y = 1::int + t1.i");
        Map<RelationName, List<Symbol>> symbolsPerRelation = HashJoinConditionSymbolsExtractor.extract(joinCondition);
        assertThat(symbolsPerRelation.get(tr1.relationName()), containsInAnyOrder(
            isReference("a"),
            isFunction(ArithmeticFunctions.Names.ADD, isReference("i"), isLiteral(1)),
            isFunction(ArithmeticFunctions.Names.ADD, isLiteral(1), isReference("i"))));
        assertThat(symbolsPerRelation.get(tr2.relationName()), containsInAnyOrder(isReference("b"), isReference("i"), isReference("y")));
    }
}
