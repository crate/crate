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

package io.crate.analyze.where;

import static io.crate.testing.Asserts.assertThat;
import static io.crate.testing.Asserts.isLiteral;

import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.RelationName;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SqlExpressions;
import io.crate.testing.T3;

public class EqualityExtractorTest extends CrateDummyClusterServiceUnitTest {

    private static final ColumnIdent x = new ColumnIdent("x");
    private static final ColumnIdent i = new ColumnIdent("i");

    private final CoordinatorTxnCtx coordinatorTxnCtx = new CoordinatorTxnCtx(CoordinatorSessionSettings.systemDefaults());
    private SqlExpressions expressions;
    private EqualityExtractor ee;

    @Before
    public void prepare() throws Exception {
        Map<RelationName, AnalyzedRelation> sources = T3.sources(List.of(T3.T1), clusterService);

        DocTableRelation tr1 = (DocTableRelation) sources.get(T3.T1);
        expressions = new SqlExpressions(sources, tr1);
        EvaluatingNormalizer normalizer = EvaluatingNormalizer.functionOnlyNormalizer(expressions.nodeCtx);
        ee = new EqualityExtractor(normalizer);
    }

    private List<List<Symbol>> analyzeParentX(Symbol query) {
        return ee.extractParentMatches(List.of(x), query, coordinatorTxnCtx);
    }

    private List<List<Symbol>> analyzeExactX(Symbol query) {
        return analyzeExact(query, List.of(x));
    }

    private List<List<Symbol>> analyzeExactXI(Symbol query) {
        return analyzeExact(query, List.of(x, i));
    }

    private List<List<Symbol>> analyzeExact(Symbol query, List<ColumnIdent> primaryKeys) {
        return ee.extractExactMatches(primaryKeys, query, coordinatorTxnCtx);
    }

    private Symbol query(String expression) {
        return expressions.normalize(expressions.asSymbol(expression));
    }

    @Test
    public void testNoExtract2ColPKWithOr() throws Exception {
        Symbol query = query("x = 1 or i = 2");
        List<List<Symbol>> matches = analyzeExactXI(query);
        assertThat(matches).isNull();
    }

    @Test
    public void testNoExtractOnNotEqualsOnSinglePk() {
        Symbol query = query("x != 1");
        List<List<Symbol>> matches = analyzeExactX(query);
        assertThat(matches).isNull();
    }

    @Test
    public void testExtract2ColPKWithAndAndNestedOr() throws Exception {
        Symbol query = query("x = 1 and (i = 2 or i = 3 or i = 4)");
        List<List<Symbol>> matches = analyzeExactXI(query);
        assertThat(matches).hasSize(3);
        assertThat(matches).satisfiesExactlyInAnyOrder(
            s -> assertThat(s).satisfiesExactly(isLiteral(1), isLiteral(2)),
            s -> assertThat(s).satisfiesExactly(isLiteral(1), isLiteral(3)),
            s -> assertThat(s).satisfiesExactly(isLiteral(1), isLiteral(4))
        );
    }

    @Test
    public void testExtract2ColPKWithOrFullDistinctKeys() throws Exception {
        Symbol query = query("(x = 1 and i = 2) or (x = 3 and i =4)");
        List<List<Symbol>> matches = analyzeExactXI(query);
        assertThat(matches).hasSize(2);
        assertThat(matches).satisfiesExactlyInAnyOrder(
            s -> assertThat(s).satisfiesExactly(isLiteral(1), isLiteral(2)),
            s -> assertThat(s).satisfiesExactly(isLiteral(3), isLiteral(4))
        );
    }

    @Test
    public void testExtract2ColPKWithOrFullDuplicateKeys() throws Exception {
        Symbol query = query("(x = 1 and i = 2) or (x = 1 and i = 4)");
        List<List<Symbol>> matches = analyzeExactXI(query);
        assertThat(matches).hasSize(2);
        assertThat(matches).satisfiesExactlyInAnyOrder(
            s -> assertThat(s).satisfiesExactly(isLiteral(1), isLiteral(2)),
            s -> assertThat(s).satisfiesExactly(isLiteral(1), isLiteral(4))
        );
    }


    @Test
    public void testExtractRoutingFromAnd() throws Exception {
        Symbol query = query("x = 1 and i = 2");
        List<List<Symbol>> matches = analyzeParentX(query);
        assertThat(matches).hasSize(1);
        assertThat(matches).satisfiesExactlyInAnyOrder(
            s -> assertThat(s).satisfiesExactly(isLiteral(1)));
    }

    @Test
    public void testExtractNoRoutingFromForeignOnly() throws Exception {
        Symbol query = query("i = 2");
        List<List<Symbol>> matches = analyzeParentX(query);
        assertThat(matches).isNull();
    }

    @Test
    public void testExtractRoutingFromOr() throws Exception {
        Symbol query = query("x = 1 or x = 2");
        List<List<Symbol>> matches = analyzeParentX(query);
        assertThat(matches).hasSize(2);
        assertThat(matches).satisfiesExactlyInAnyOrder(
            s -> assertThat(s).satisfiesExactly(isLiteral(1)),
            s -> assertThat(s).satisfiesExactly(isLiteral(2)));
    }

    @Test
    public void testNoExtractSinglePKFromAnd() throws Exception {
        Symbol query = query("x = 1 and x = 2");
        List<List<Symbol>> matches = analyzeExactX(query);
        assertThat(matches).isNull();
    }


    @Test
    public void testExtractRoutingFromNestedOr() throws Exception {
        Symbol query = query("x =1 or x =2 or x = 3 or x = 4");
        List<List<Symbol>> matches = analyzeParentX(query);
        assertThat(matches).hasSize(4);
        assertThat(matches).satisfiesExactlyInAnyOrder(
            s -> assertThat(s).satisfiesExactly(isLiteral(1)),
            s -> assertThat(s).satisfiesExactly(isLiteral(2)),
            s -> assertThat(s).satisfiesExactly(isLiteral(3)),
            s -> assertThat(s).satisfiesExactly(isLiteral(4)));
    }

    @Test
    public void testExtractNoRoutingFromOrWithForeignColumn() throws Exception {
        Symbol query = query("x = 1 or i = 2");
        List<List<Symbol>> matches = analyzeParentX(query);
        assertThat(matches).isNull();
    }

    @Test
    public void testExtract2ColPKFromNestedOrWithDuplicates() throws Exception {
        Symbol query = query("x = 1 and (i = 2 or i = 2 or i = 4)");
        List<List<Symbol>> matches = analyzeExactXI(query);
        assertThat(matches).hasSize(2);
        assertThat(matches).satisfiesExactlyInAnyOrder(
            s -> assertThat(s).satisfiesExactly(isLiteral(1), isLiteral(2)),
            s -> assertThat(s).satisfiesExactly(isLiteral(1), isLiteral(4)));
    }


    @Test
    public void testNoExtract2ColPKFromAndEq1PartAnd2ForeignColumns() throws Exception {
        Symbol query = query("x = 1 and (i = 2 or a = 'a')");
        List<List<Symbol>> matches = analyzeExactXI(query);
        assertThat(matches).isNull();
    }


    /**
     * x=1 and (y=2 or ?)
     * and(x1, or(y2, ?)
     * <p>
     * x = 1 and (y=2 or x=3)
     * and(x1, or(y2, x3)
     * <p>
     * x=1 and (y=2 or y=3)
     * and(x1, or(or(y2, y3), y4))
     * <p>
     * branches: x1,
     * <p>
     * <p>
     * <p>
     * x=1 and (y=2 or F)
     * 1,2   1=1 and (2=2 or z=3) T
     */
    @Test
    public void testNoExtract2ColPKFromAndWithEq1PartAnd1ForeignColumnInOr() throws Exception {
        Symbol query = query("x = 1 and (i = 2 or a = 'a')");
        List<List<Symbol>> matches = analyzeExactXI(query);
        assertThat(matches).isNull();
    }

    @Test
    public void testExtract2ColPKFrom1PartAndOtherPart2EqOr() throws Exception {
        Symbol query = query("x = 1 and (i = 2 or i = 3)");
        List<List<Symbol>> matches = analyzeExactXI(query);
        assertThat(matches).hasSize(2);
        assertThat(matches).satisfiesExactlyInAnyOrder(
            s -> assertThat(s).satisfiesExactly(isLiteral(1), isLiteral(2)),
            s -> assertThat(s).satisfiesExactly(isLiteral(1), isLiteral(3)));
    }

    @Test
    public void testNoExtract2ColPKFromOnly1Part() throws Exception {
        Symbol query = query("x = 1");
        List<List<Symbol>> matches = analyzeExactXI(query);
        assertThat(matches).isNull();
    }

    @Test
    public void testExtractSinglePKFromAnyEq() throws Exception {
        Symbol query = query("x = any([1, 2, 3])");
        List<List<Symbol>> matches = analyzeExactX(query);
        assertThat(matches).hasSize(3);
        assertThat(matches).satisfiesExactlyInAnyOrder(
            s -> assertThat(s).satisfiesExactly(isLiteral(1)),
            s -> assertThat(s).satisfiesExactly(isLiteral(2)),
            s -> assertThat(s).satisfiesExactly(isLiteral(3)));
    }

    @Test
    public void testExtract2ColPKFromAnyEq() throws Exception {
        Symbol query = query("i = 4 and x = any([1, 2, 3])");
        List<List<Symbol>> matches = analyzeExactXI(query);
        assertThat(matches).hasSize(3);
        assertThat(matches).satisfiesExactlyInAnyOrder(
            s -> assertThat(s).satisfiesExactly(isLiteral(1), isLiteral(4)),
            s -> assertThat(s).satisfiesExactly(isLiteral(2), isLiteral(4)),
            s -> assertThat(s).satisfiesExactly(isLiteral(3), isLiteral(4)));
    }

    @Test
    public void testExtractSinglePKFromAnyEqInOr() throws Exception {
        Symbol query = query("x = any([1, 2, 3]) or x = any([4, 5, 3])");
        List<List<Symbol>> matches = analyzeExactX(query);
        assertThat(matches).hasSize(5);
        assertThat(matches).satisfiesExactlyInAnyOrder(
            s -> assertThat(s).satisfiesExactly(isLiteral(1)),
            s -> assertThat(s).satisfiesExactly(isLiteral(2)),
            s -> assertThat(s).satisfiesExactly(isLiteral(3)),
            s -> assertThat(s).satisfiesExactly(isLiteral(4)),
            s -> assertThat(s).satisfiesExactly(isLiteral(5)));
    }

    @Test
    public void testExtractSinglePKFromOrInAnd() throws Exception {
        Symbol query = query("(x = 1 or x = 2 or x = 3) and (x = 1 or x = 4 or x = 5)");
        List<List<Symbol>> matches = analyzeExactX(query);
        assertThat(matches).hasSize(1);
        assertThat(matches).satisfiesExactlyInAnyOrder(
            s -> assertThat(s).satisfiesExactly(isLiteral(1)));
    }

    @Test
    public void testExtractSinglePK1FromAndAnyEq() throws Exception {
        Symbol query = query("x = any([1, 2, 3]) and x = any([4, 5, 3])");
        List<List<Symbol>> matches = analyzeExactX(query);
        assertThat(matches).hasSize(1);
        assertThat(matches).satisfiesExactlyInAnyOrder(
            s -> assertThat(s).satisfiesExactly(isLiteral(3)));
    }

    @Test
    public void testExtract2ColPKFromAnyEqAnd() throws Exception {
        Symbol query = query("x = any([1, 2, 3]) and i = any([1, 2, 3])");
        List<List<Symbol>> matches = analyzeExactXI(query);
        assertThat(matches).hasSize(9); // cartesian product: 3 * 3
        assertThat(matches).satisfiesExactlyInAnyOrder(
            s -> assertThat(s).satisfiesExactly(isLiteral(1), isLiteral(1)),
            s -> assertThat(s).satisfiesExactly(isLiteral(1), isLiteral(2)),
            s -> assertThat(s).satisfiesExactly(isLiteral(1), isLiteral(3)),
            s -> assertThat(s).satisfiesExactly(isLiteral(2), isLiteral(1)),
            s -> assertThat(s).satisfiesExactly(isLiteral(2), isLiteral(2)),
            s -> assertThat(s).satisfiesExactly(isLiteral(2), isLiteral(3)),
            s -> assertThat(s).satisfiesExactly(isLiteral(3), isLiteral(1)),
            s -> assertThat(s).satisfiesExactly(isLiteral(3), isLiteral(2)),
            s -> assertThat(s).satisfiesExactly(isLiteral(3), isLiteral(3)));
    }

    @Test
    public void testNoPKExtractionIfMatchIsPresent() throws Exception {
        Symbol query = query("x in (1, 2, 3) and match(a, 'Hello World')");
        List<List<Symbol>> matches = analyzeExactX(query);
        assertThat(matches).isNull();
    }

    @Test
    public void testNoPKExtractionOnNotIn() {
        List<List<Symbol>> matches = analyzeExactX(query("x not in (1, 2, 3)"));
        assertThat(matches).isNull();
    }

    @Test
    public void testNoPKExtractionWhenColumnsOnBothSidesOfEqual() {
        List<List<Symbol>> matches = analyzeExactX(query("x = abs(x)"));
        assertThat(matches).isNull();
    }

    @Test
    public void test_primary_key_comparison_is_detected_inside_cast_function() throws Exception {
        Symbol query = query("cast(x as bigint) = 0");
        List<List<Symbol>> matches = analyzeExactX(query);
        assertThat(matches).hasSize(1);
        assertThat(matches).satisfiesExactlyInAnyOrder(
            s -> assertThat(s).satisfiesExactly(isLiteral(0L)));
    }

    @Test
    public void test_primary_key_extraction_on_subscript_with_any() {
        Map<RelationName, AnalyzedRelation> sources = T3.sources(List.of(T3.T4), clusterService);
        DocTableRelation tr4 = (DocTableRelation) sources.get(T3.T4);
        var expressionsT4 = new SqlExpressions(sources, tr4);
        var pkCol = new ColumnIdent("obj");

        var query = expressionsT4.normalize(expressionsT4.asSymbol("obj = any([{i = 1}])"));
        List<List<Symbol>> matches = analyzeExact(query, List.of(pkCol));
        assertThat(matches).hasSize(1);
        assertThat(matches).satisfiesExactlyInAnyOrder(
            s -> assertThat(s).satisfiesExactly(isLiteral(Map.of("i", 1))));
    }

    @Test
    public void test_primary_key_extraction_if_combined_with_and_operator() throws Exception {
        Symbol query = query("x = 1 and a = 'foo' or (x = 3 and a = 'bar')");
        List<List<Symbol>> matches = analyzeExactX(query);
        assertThat(matches).hasSize(2);
        assertThat(matches).satisfiesExactlyInAnyOrder(
            s -> assertThat(s).satisfiesExactly(isLiteral(1)),
            s -> assertThat(s).satisfiesExactly(isLiteral(3)));
    }

    @Test
    public void test_primary_key_extraction_if_combined_with_and_scalar() throws Exception {
        Symbol query = query("x in (1, 2, 3) and substr(cast(x as string), 0) = 4");
        List<List<Symbol>> matches = analyzeExactX(query);
        assertThat(matches).hasSize(3);
        assertThat(matches).satisfiesExactlyInAnyOrder(
            s -> assertThat(s).satisfiesExactly(isLiteral(1)),
            s -> assertThat(s).satisfiesExactly(isLiteral(2)),
            s -> assertThat(s).satisfiesExactly(isLiteral(3)));
    }
}
