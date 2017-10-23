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

package io.crate.action.sql;

import io.crate.analyze.QuerySpec;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.ParameterSymbol;
import io.crate.analyze.symbol.Symbol;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class SessionTest extends CrateUnitTest {

    @Test
    public void testParameterTypeExtractorNotApplicable() {
        Session.ParameterTypeExtractor typeExtractor = new Session.ParameterTypeExtractor();

        assertThat(typeExtractor.getParameterTypes(null), is(nullValue()));

        AnalyzedRelation analyzedRelation = Mockito.mock(DocTableRelation.class);
        assertThat(typeExtractor.getParameterTypes(analyzedRelation), is(nullValue()));
    }

    @Test
    public void testParameterTypeExtractor() {
        Session.ParameterTypeExtractor typeExtractor = new Session.ParameterTypeExtractor();
        List<Symbol> symbolsToVisit = new ArrayList<>();
        symbolsToVisit.add(Literal.of(1));
        symbolsToVisit.add(Literal.of("foo"));
        symbolsToVisit.add(new ParameterSymbol(1, DataTypes.LONG));
        symbolsToVisit.add(new ParameterSymbol(0, DataTypes.INTEGER));
        symbolsToVisit.add(new ParameterSymbol(3, DataTypes.STRING));
        symbolsToVisit.add(Literal.of("bar"));
        symbolsToVisit.add(new ParameterSymbol(2, DataTypes.DOUBLE));
        symbolsToVisit.add(new ParameterSymbol(1, DataTypes.LONG));
        symbolsToVisit.add(new ParameterSymbol(0, DataTypes.INTEGER));
        symbolsToVisit.add(Literal.of(1.2));

        QueriedRelation analyzedRelation = Mockito.mock(QueriedRelation.class);
        Mockito.when(analyzedRelation.querySpec()).thenReturn(new MyQuerySpec(symbolsToVisit));

        DataType[] parameterTypes = typeExtractor.getParameterTypes(analyzedRelation);
        assertThat(parameterTypes, equalTo(new DataType[] {
            DataTypes.INTEGER,
            DataTypes.LONG,
            DataTypes.DOUBLE,
            DataTypes.STRING,
        }));

        symbolsToVisit.add(new ParameterSymbol(4, DataTypes.BOOLEAN));
        parameterTypes = typeExtractor.getParameterTypes(analyzedRelation);
        assertThat(parameterTypes, equalTo(new DataType[] {
            DataTypes.INTEGER,
            DataTypes.LONG,
            DataTypes.DOUBLE,
            DataTypes.STRING,
            DataTypes.BOOLEAN
        }));

        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("The assembled list of ParameterSymbols is invalid.");
        // remove the double parameter => make the input invalid
        symbolsToVisit.remove(6);
        typeExtractor.getParameterTypes(analyzedRelation);
    }

    private static class MyQuerySpec extends QuerySpec {

        private final List<Symbol> symbolsToVisit;

        private MyQuerySpec(List<Symbol> symbolsToVisit) {
            this.symbolsToVisit = symbolsToVisit;
        }

        @Override
        public void visitSymbols(Consumer<? super Symbol> consumer) {
            for (Symbol symbol : symbolsToVisit) {
                consumer.accept(symbol);
            }
        }
    }
}
