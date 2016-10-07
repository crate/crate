/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.analyze.where;

import com.google.common.collect.ImmutableList;
import io.crate.analyze.BaseAnalyzerTest;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.*;
import io.crate.operation.operator.AndOperator;
import io.crate.operation.operator.EqOperator;
import io.crate.operation.operator.OperatorModule;
import io.crate.operation.operator.OrOperator;
import io.crate.operation.operator.any.AnyEqOperator;
import io.crate.testing.MockedClusterServiceModule;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.inject.Module;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static io.crate.testing.TestingHelpers.isLiteral;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;

@SuppressWarnings("unchecked")
public class EqualityExtractorTest extends BaseAnalyzerTest {

    TransactionContext transactionContext = new TransactionContext();

    @Override
    protected List<Module> getModules() {
        List<Module> modules = super.getModules();
        modules.addAll(Arrays.<Module>asList(
            new MockedClusterServiceModule(),
            new MetaDataModule(),
            new OperatorModule())
        );
        return modules;
    }


    private List<List<Symbol>> analyzeParentX(Symbol query) {
        return getExtractor().extractParentMatches(ImmutableList.of(Ref("x").ident().columnIdent()), query, transactionContext);

    }

    private List<List<Symbol>> analyzeExactX(Symbol query) {
        return analyzeExact(query, ImmutableList.of(Ref("x").ident().columnIdent()));
    }


    private List<List<Symbol>> analyzeExactXY(Symbol query) {
        return analyzeExact(query, ImmutableList.of(Ref("x").ident().columnIdent(), Ref("y").ident().columnIdent()));
    }

    private List<List<Symbol>> analyzeExact(Symbol query, List<ColumnIdent> cols) {
        EqualityExtractor ee = getExtractor();
        return ee.extractExactMatches(cols, query, transactionContext);
    }

    private EqualityExtractor getExtractor() {
        EvaluatingNormalizer normalizer = new EvaluatingNormalizer(
            injector.getInstance(Functions.class),
            RowGranularity.CLUSTER,
            injector.getInstance(NestedReferenceResolver.class),
            null,
            ReplaceMode.COPY);

        return new EqualityExtractor(normalizer);
    }


    private Function And(Symbol left, Symbol right) {
        return new Function(AndOperator.INFO, Arrays.asList(left, right));
    }

    private Function AnyEq(Symbol left, Symbol right) {
        return new Function(
            new FunctionInfo(
                new FunctionIdent(AnyEqOperator.NAME, ImmutableList.of(left.valueType(), right.valueType())),
                DataTypes.BOOLEAN
            ),
            Arrays.asList(left, right)
        );
    }

    private Function Or(Symbol left, Symbol right) {
        return new Function(OrOperator.INFO, Arrays.asList(left, right));
    }

    private Reference Ref(String name) {
        return Ref(name, DataTypes.STRING);
    }

    private Reference Ref(String name, DataType type) {
        return new Reference(
            new ReferenceIdent(new TableIdent("testschema", "test"), name),
            RowGranularity.DOC,
            type
        );
    }

    private Function Eq(Symbol left, Symbol right) {
        FunctionInfo info = new FunctionInfo(
            new FunctionIdent(EqOperator.NAME, ImmutableList.of(left.valueType(), right.valueType())),
            DataTypes.BOOLEAN
        );
        return new Function(info, Arrays.asList(left, right));
    }

    private Function Eq(String name, Integer i) {
        return Eq(Ref(name), Literal.of(i));
    }

    @Test
    public void testNoExtract2ColPKWithOr() throws Exception {
        Symbol query = Or(
            Eq(Ref("x"), Literal.of(1)),
            Eq(Ref("y"), Literal.of(2))
        );

        List<List<Symbol>> matches = analyzeExactXY(query);
        assertNull(matches);
    }

    @Test
    public void testExtract2ColPKWithAndAndNestedOr() throws Exception {
        Symbol query = And(
            Eq("x", 1),
            Or(Or(Eq("y", 2), Eq("y", 3)), Eq("y", 4))
        );
        List<List<Symbol>> matches = analyzeExactXY(query);
        assertThat(matches.size(), is(3));
        assertThat(matches, containsInAnyOrder(
            contains(isLiteral(1), isLiteral(2)),
            contains(isLiteral(1), isLiteral(3)),
            contains(isLiteral(1), isLiteral(4)))
        );
    }

    @Test
    public void testExtract2ColPKWithOrFullDistinctKeys() throws Exception {
        Symbol query = Or(
            And(Eq("x", 1), Eq("y", 2)),
            And(Eq("x", 3), Eq("y", 4))
        );
        List<List<Symbol>> matches = analyzeExactXY(query);
        assertThat(matches.size(), is(2));

        assertThat(matches, containsInAnyOrder(
            contains(isLiteral(1), isLiteral(2)),
            contains(isLiteral(3), isLiteral(4))
        ));
    }

    @Test
    public void testExtract2ColPKWithOrFullDuplicateKeys() throws Exception {
        Symbol query = Or(
            And(Eq("x", 1), Eq("y", 2)),
            And(Eq("x", 1), Eq("y", 4))
        );
        List<List<Symbol>> matches = analyzeExactXY(query);
        assertThat(matches.size(), is(2));
        assertThat(matches, containsInAnyOrder(
            contains(isLiteral(1), isLiteral(2)),
            contains(isLiteral(1), isLiteral(4))
        ));
    }


    @Test
    public void testExtractRoutingFromAnd() throws Exception {
        Symbol query = And(
            Eq("x", 1),
            Eq("y", 2)
        );
        List<List<Symbol>> matches = analyzeParentX(query);
        assertThat(matches.size(), is(1));

        assertThat(matches, contains(
            contains(isLiteral(1))
        ));
    }

    @Test
    public void testExtractNoRoutingFromForeignOnly() throws Exception {
        Symbol query = Eq("y", 2);
        List<List<Symbol>> matches = analyzeParentX(query);
        assertNull(matches);
    }

    @Test
    public void testExtractRoutingFromOr() throws Exception {
        Symbol query = Or(
            Eq("x", 1),
            Eq("x", 2)
        );
        List<List<Symbol>> matches = analyzeParentX(query);
        assertThat(matches.size(), is(2));
        assertThat(matches, containsInAnyOrder(
            contains(isLiteral(1)),
            contains(isLiteral(2))
        ));
    }

    @Test
    public void testNoExtractSinglePKFromAnd() throws Exception {
        Symbol query = And(
            Eq("x", 1),
            Eq("x", 2)
        );
        List<List<Symbol>> matches = analyzeExactX(query);
        assertNull(matches);
    }


    @Test
    public void testExtractRoutingFromNestedOr() throws Exception {
        Symbol query = Or(Eq("x", 1),
            Or(Or(Eq("x", 2), Eq("x", 3)), Eq("x", 4))
        );
        List<List<Symbol>> matches = analyzeParentX(query);
        assertThat(matches.size(), is(4));

        assertThat(matches, containsInAnyOrder(
            contains(isLiteral(1)),
            contains(isLiteral(2)),
            contains(isLiteral(3)),
            contains(isLiteral(4))
        ));
    }

    @Test
    public void testExtractNoRoutingFromOrWithForeignColumn() throws Exception {
        Symbol query = Or(
            Eq("x", 1),
            Eq("a", 2)
        );
        List<List<Symbol>> matches = analyzeParentX(query);
        assertNull(matches);
    }

    @Test
    public void testNoExtractSinglePKFromAndWithForeignColumn() throws Exception {
        // x=1 or (x=2 and a=2)
        Symbol query = Or(
            Eq("x", 1),
            And(Eq("x", 2),
                Eq("a", 2))
        );
        List<List<Symbol>> matches = analyzeExactX(query);
        assertNull(matches);
    }


    @Test
    public void testExtract2ColPKFromNestedOrWithDuplicates() throws Exception {
        Symbol query = And(
            Eq("x", 1),
            Or(Or(Eq("y", 2), Eq("y", 2)), Eq("y", 4))
        );
        List<List<Symbol>> matches = analyzeExactXY(query);
        assertThat(matches.size(), is(2));

        assertThat(matches, containsInAnyOrder(
            contains(isLiteral(1), isLiteral(2)),
            contains(isLiteral(1), isLiteral(4))
        ));
    }


    @Test
    public void testNoExtract2ColPKFromAndEq1PartAnd2ForeignColumns() throws Exception {
        Symbol query = And(
            Eq(Ref("x"), Literal.of(1)),
            Or(
                Eq(Ref("a"), Literal.of(2)),
                Eq(Ref("z"), Literal.of(3))
            ));

        List<List<Symbol>> matches = analyzeExactXY(query);
        assertNull(matches);
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
        Symbol query = And(
            Eq("x", 1),
            Or(
                Eq("y", 2),
                Eq("z", 3)
            ));

        List<List<Symbol>> matches = analyzeExactXY(query);
        assertNull(matches);
    }

    @Test
    public void testExtract2ColPKFrom1PartAndOtherPart2EqOr() throws Exception {
        Symbol query = And(
            Eq(Ref("x"), Literal.of(1)),
            Or(
                Eq(Ref("y"), Literal.of(2)),
                Eq(Ref("y"), Literal.of(3))
            ));

        List<List<Symbol>> matches = analyzeExactXY(query);
        assertThat(matches.size(), is(2));
        assertThat(matches, containsInAnyOrder(
            contains(isLiteral(1), isLiteral(2)),
            contains(isLiteral(1), isLiteral(3))
        ));
    }

    @Test
    public void testNoExtract2ColPKFromOnly1Part() throws Exception {
        Symbol query = Eq(Ref("x"), Literal.of(1));
        List<List<Symbol>> matches = analyzeExactXY(query);
        assertNull(matches);
    }

    @Test
    public void testExtractSinglePKFromAnyEq() throws Exception {
        Symbol query = AnyEq(Ref("x"), Literal.of(new ArrayType(DataTypes.STRING), new Object[]{new BytesRef("a"), new BytesRef("b"), new BytesRef("c")}));
        List<List<Symbol>> matches = analyzeExactX(query);
        assertThat(matches.size(), is(3));
        assertThat(matches, containsInAnyOrder(
            contains(isLiteral("a")),
            contains(isLiteral("b")),
            contains(isLiteral("c"))
        ));
    }

    @Test
    public void testExtract2ColPKFromAnyEq() throws Exception {
        Symbol query = And(
            AnyEq(Ref("x"), Literal.of(new ArrayType(DataTypes.STRING), new Object[]{new BytesRef("a"), new BytesRef("b"), new BytesRef("c")})),
            Eq("y", 4));
        List<List<Symbol>> matches = analyzeExactXY(query);
        assertThat(matches.size(), is(3));
        assertThat(matches, containsInAnyOrder(
            contains(isLiteral("a"), isLiteral(4)),
            contains(isLiteral("b"), isLiteral(4)),
            contains(isLiteral("c"), isLiteral(4))
        ));


    }

    @Test
    public void testExtractSinglePKFromAnyEqInOr() throws Exception {
        Symbol query = Or(
            AnyEq(Ref("x"), Literal.of(new ArrayType(DataTypes.STRING), new Object[]{new BytesRef("a"), new BytesRef("b"), new BytesRef("c")})),
            AnyEq(Ref("x"), Literal.of(new ArrayType(DataTypes.STRING), new Object[]{new BytesRef("d"), new BytesRef("e"), new BytesRef("c")}))
        );
        List<List<Symbol>> matches = analyzeExactX(query);
        assertThat(matches.size(), is(5));
        assertThat(matches, containsInAnyOrder(
            contains(isLiteral("a")),
            contains(isLiteral("b")),
            contains(isLiteral("c")),
            contains(isLiteral("d")),
            contains(isLiteral("e"))

        ));
    }

    @Test
    public void testExtractSinglePKFromOrInAnd() throws Exception {
        Symbol query = And(
            Or(Eq("x", 1), Or(Eq("x", 2), Eq("x", 3))),
            Or(Eq("x", 1), Or(Eq("x", 4), Eq("x", 5)))
        );
        List<List<Symbol>> matches = analyzeExactX(query);
        assertThat(matches.size(), is(1));
        assertThat(matches, contains(
            contains(isLiteral(1))
        ));
    }

    @Test
    public void testExtractSinglePK1FromAndAnyEq() throws Exception {
        Symbol query = And(
            AnyEq(Ref("x"), Literal.of(new ArrayType(DataTypes.STRING), new Object[]{new BytesRef("a"), new BytesRef("b"), new BytesRef("c")})),
            AnyEq(Ref("x"), Literal.of(new ArrayType(DataTypes.STRING), new Object[]{new BytesRef("d"), new BytesRef("e"), new BytesRef("c")}))
        );
        List<List<Symbol>> matches = analyzeExactX(query);
        assertThat(matches, is(notNullValue()));
        assertThat(matches.size(), is(1)); // STRING c
        assertThat(matches, contains(
            contains(isLiteral(new BytesRef("c")))
        ));
    }

    @Test
    public void testExtract2ColPKFromAnyEqAnd() throws Exception {
        Symbol query = And(
            AnyEq(Ref("x"), Literal.of(new ArrayType(DataTypes.STRING), new Object[]{new BytesRef("a"), new BytesRef("b"), new BytesRef("c")})),
            AnyEq(Ref("y"), Literal.of(new ArrayType(DataTypes.STRING), new Object[]{new BytesRef("a"), new BytesRef("b"), new BytesRef("c")}))
        );
        List<List<Symbol>> matches = analyzeExactXY(query);
        assertThat(matches.size(), is(9)); // cartesian product: 3 * 3
        assertThat(matches, containsInAnyOrder(
            contains(isLiteral("a"), isLiteral("a")),
            contains(isLiteral("a"), isLiteral("b")),
            contains(isLiteral("a"), isLiteral("c")),
            contains(isLiteral("b"), isLiteral("a")),
            contains(isLiteral("b"), isLiteral("b")),
            contains(isLiteral("b"), isLiteral("c")),
            contains(isLiteral("c"), isLiteral("a")),
            contains(isLiteral("c"), isLiteral("b")),
            contains(isLiteral("c"), isLiteral("c"))
        ));
    }
}
