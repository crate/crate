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
import io.crate.metadata.*;
import io.crate.operation.operator.AndOperator;
import io.crate.operation.operator.EqOperator;
import io.crate.operation.operator.OperatorModule;
import io.crate.operation.operator.OrOperator;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.testing.MockedClusterServiceModule;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.inject.Module;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.core.Is.is;

public class EqualityExtractorTest extends BaseAnalyzerTest {

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
        return getExtractor().extractParentMatches(ImmutableList.of(Ref("x").ident().columnIdent()), query);

    }

    private List<List<Symbol>> analyzeExactX(Symbol query) {
        return analyzeExact(query, ImmutableList.of(Ref("x").ident().columnIdent()));
    }


    private List<List<Symbol>> analyzeExactXY(Symbol query) {
        return analyzeExact(query, ImmutableList.of(Ref("x").ident().columnIdent(), Ref("y").ident().columnIdent()));
    }

    private List<List<Symbol>> analyzeExact(Symbol query, List<ColumnIdent> cols) {
        EqualityExtractor ee = getExtractor();
        return ee.extractExactMatches(cols, query);
    }

    private EqualityExtractor getExtractor() {
        EvaluatingNormalizer normalizer = new EvaluatingNormalizer(
                injector.getInstance(Functions.class),
                RowGranularity.CLUSTER,
                injector.getInstance(ReferenceResolver.class));

        return new EqualityExtractor(normalizer);
    }


    private Function And(Symbol left, Symbol right) {
        return new Function(AndOperator.INFO, Arrays.asList(left, right));
    }

    private Function Or(Symbol left, Symbol right) {
        return new Function(OrOperator.INFO, Arrays.asList(left, right));
    }

    private Reference Ref(String name) {
        return Ref(name, DataTypes.STRING);
    }

    private Reference Ref(String name, DataType type) {
        return new Reference(new ReferenceInfo(
                new ReferenceIdent(new TableIdent("testschema", "test"), name),
                RowGranularity.DOC,
                type
        ));
    }

    private Function Eq(Symbol left, Symbol right) {
        FunctionInfo info = new FunctionInfo(
                new FunctionIdent(EqOperator.NAME, ImmutableList.of(left.valueType(), right.valueType())),
                DataTypes.BOOLEAN
        );
        return new Function(info, Arrays.asList(left, right));
    }

    private Function Eq(String name, Integer i) {
        return Eq(Ref(name), Literal.newLiteral(i));
    }

    @Test
    public void testPK2_2ColOr() throws Exception {
        Symbol query = Or(
                Eq(Ref("x"), Literal.newLiteral(1)),
                Eq(Ref("y"), Literal.newLiteral(2))
        );

        List<List<Symbol>> matches = analyzeExactXY(query);
        assertNull(matches);
    }


    @Test
    public void testPK2_NestedOr() throws Exception {
        Symbol query = And(
                Eq("x", 1),
                Or(Or(Eq("y", 2), Eq("y", 3)), Eq("y", 4))
        );
        List<List<Symbol>> matches = analyzeExactXY(query);
        assertThat(matches.size(), is(3));

        for (List<Symbol> match : matches) {
            System.out.println(match);
        }
    }

    @Test
    public void testPK2_OrFullDistinctKeys() throws Exception {
        Symbol query = Or(
                And(Eq("x", 1), Eq("y", 2)),
                And(Eq("x", 3), Eq("y", 4))
        );
        List<List<Symbol>> matches = analyzeExactXY(query);
        assertThat(matches.size(), is(2));

        for (List<Symbol> match : matches) {
            System.out.println(match);
        }
    }

    @Test
    public void testPK2_OrFullDuplicateKeys() throws Exception {
        Symbol query = Or(
                And(Eq("x", 1), Eq("y", 2)),
                And(Eq("x", 1), Eq("y", 4))
        );
        List<List<Symbol>> matches = analyzeExactXY(query);
        assertThat(matches.size(), is(2));
        for (List<Symbol> match : matches) {
            System.out.println(match);
        }
    }


    @Test
    public void testRoutingAnd() throws Exception {
        Symbol query = And(
                Eq("x", 1),
                Eq("y", 2)
        );
        List<List<Symbol>> matches = analyzeParentX(query);
        assertThat(matches.size(), is(1));

        for (List<Symbol> match : matches) {
            System.out.println(match);
        }
    }

    @Test
    public void testRoutingForeignOnly() throws Exception {
        Symbol query = Eq("y", 2);
        List<List<Symbol>> matches = analyzeParentX(query);
        assertNull(matches);
    }

    @Test
    public void testRoutingOr() throws Exception {
        Symbol query = Or(
                Eq("x", 1),
                Eq("x", 2)
        );
        List<List<Symbol>> matches = analyzeParentX(query);
        assertThat(matches.size(), is(2));
    }

    @Test
    public void testPKAnd() throws Exception {
        Symbol query = And(
                Eq("x", 1),
                Eq("x", 2)
        );
        List<List<Symbol>> matches = analyzeExactX(query);
        assertNull(matches);
    }



    @Test
    public void testRoutingOrNested() throws Exception {
        Symbol query = Or(Eq("x", 1),
                Or(Or(Eq("x", 2), Eq("x", 3)), Eq("x", 4))
        );
        List<List<Symbol>> matches = analyzeParentX(query);
        assertThat(matches.size(), is(4));
    }

    @Test
    public void testRoutingOrForeign() throws Exception {
        Symbol query = Or(
                Eq("x", 1),
                Eq("a", 2)
        );
        List<List<Symbol>> matches = analyzeParentX(query);
        assertNull(matches);
    }

    @Test
    public void testPK1AndForeign() throws Exception {
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
    public void testPK2_NestedOrWithDuplicates() throws Exception {
        Symbol query = And(
                Eq("x", 1),
                Or(Or(Eq("y", 2), Eq("y", 2)), Eq("y", 4))
        );
        List<List<Symbol>> matches = analyzeExactXY(query);
        assertThat(matches.size(), is(2));

        for (List<Symbol> match : matches) {
            System.out.println(match);
        }
    }


    @Test
    public void testPK2_Eq1_Foreign2() throws Exception {
        Symbol query = And(
                Eq(Ref("x"), Literal.newLiteral(1)),
                Or(
                        Eq(Ref("a"), Literal.newLiteral(2)),
                        Eq(Ref("z"), Literal.newLiteral(3))
                ));

        List<List<Symbol>> matches = analyzeExactXY(query);
        assertNull(matches);
    }


    /**
     * x=1 and (y=2 or ?)
     * and(x1, or(y2, ?)
     *
     * x = 1 and (y=2 or x=3)
     * and(x1, or(y2, x3)
     *
     * x=1 and (y=2 or y=3)
     * and(x1, or(or(y2, y3), y4))
     *
     * branches: x1,
     *
     *
     *
     * x=1 and (y=2 or F)
     * 1,2   1=1 and (2=2 or z=3) T
     *
     */
    @Test
    public void testPK2_Eq1_Foreign1() throws Exception {
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
    public void testPK2_3EqAndOr() throws Exception {
        Symbol query = And(
                Eq(Ref("x"), Literal.newLiteral(1)),
                Or(
                        Eq(Ref("y"), Literal.newLiteral(2)),
                        Eq(Ref("y"), Literal.newLiteral(3))
                ));

        List<List<Symbol>> matches = analyzeExactXY(query);
        assertThat(matches.size(), is(2));
        for (List<Symbol> match : matches) {
            System.out.println(match);
        }
    }


    @Test
    public void testPK2_Eq1() throws Exception {
        Symbol query = Eq(Ref("x"), Literal.newLiteral(1));
        List<List<Symbol>> matches = analyzeExactXY(query);
        assertNull(matches);
    }

}