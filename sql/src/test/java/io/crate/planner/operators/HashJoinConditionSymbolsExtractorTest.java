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
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.SqlExpressions;
import io.crate.testing.T3;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static io.crate.testing.SymbolMatchers.isField;
import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isLiteral;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class HashJoinConditionSymbolsExtractorTest extends CrateUnitTest {

    private static final SqlExpressions SQL_EXPRESSIONS = new SqlExpressions(T3.SOURCES);

    @Test
    public void testExtractFromTopEqCondition() {
        Symbol joinCondition = SQL_EXPRESSIONS.asSymbol("t1.x = t2.y");
        Map<AnalyzedRelation, List<Symbol>> symbolsPerRelation = HashJoinConditionSymbolsExtractor.extract(joinCondition);
        assertThat(symbolsPerRelation.get(T3.TR_1), contains(isField("x")));
        assertThat(symbolsPerRelation.get(T3.TR_2), contains(isField("y")));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testExtractFromNestedEqCondition() {
        Symbol joinCondition = SQL_EXPRESSIONS.asSymbol("t1.x > t2.y and t1.a = t2.b and not(t1.i = t2.i)");
        Map<AnalyzedRelation, List<Symbol>> symbolsPerRelation = HashJoinConditionSymbolsExtractor.extract(joinCondition);
        assertThat(symbolsPerRelation.get(T3.TR_1), containsInAnyOrder(isField("a"), isField("i")));
        assertThat(symbolsPerRelation.get(T3.TR_2), containsInAnyOrder(isField("b"), isField("i")));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testExtractSymbolsWithDuplicates() {
        Symbol joinCondition = SQL_EXPRESSIONS.asSymbol("t1.a = t2.b and t1.i + 1 = t2.i and t2.y = t1.i + 1");
        Map<AnalyzedRelation, List<Symbol>> symbolsPerRelation = HashJoinConditionSymbolsExtractor.extract(joinCondition);
        assertThat(symbolsPerRelation.get(T3.TR_1), containsInAnyOrder(
            isField("a"),
            isFunction(ArithmeticFunctions.Names.ADD, isField("i"), isLiteral(1))));
        assertThat(symbolsPerRelation.get(T3.TR_2), containsInAnyOrder(isField("b"), isField("i")));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testExtractRelationsOfFunctionsWithLiterals() {
        Symbol joinCondition = SQL_EXPRESSIONS.asSymbol("t1.a = t2.b and t1.i + 1 = t2.i and t2.y = 1 + t1.i");
        Map<AnalyzedRelation, List<Symbol>> symbolsPerRelation = HashJoinConditionSymbolsExtractor.extract(joinCondition);
        assertThat(symbolsPerRelation.get(T3.TR_1), containsInAnyOrder(
            isField("a"),
            isFunction(ArithmeticFunctions.Names.ADD, isField("i"), isLiteral(1)),
            isFunction(ArithmeticFunctions.Names.ADD, isLiteral(1), isField("i"))));
        assertThat(symbolsPerRelation.get(T3.TR_2), containsInAnyOrder(isField("b"), isField("i"), isField("y")));
    }
}
