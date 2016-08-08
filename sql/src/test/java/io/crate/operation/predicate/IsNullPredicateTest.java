/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operation.predicate;

import io.crate.analyze.symbol.DynamicReference;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.*;
import io.crate.operation.Input;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.junit.Test;

import java.util.Arrays;

import static io.crate.testing.TestingHelpers.isLiteral;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.core.Is.is;

public class IsNullPredicateTest extends CrateUnitTest {

    IsNullPredicate predicate = new IsNullPredicate(new FunctionInfo(
            new FunctionIdent(IsNullPredicate.NAME, Arrays.<DataType>asList(DataTypes.STRING)),
            DataTypes.BOOLEAN
    ));

    private final StmtCtx stmtCtx = new StmtCtx();

    @Test
    public void testNormalizeSymbolFalse() throws Exception {
        Function isNull = new Function(predicate.info(), Arrays.<Symbol>asList(Literal.newLiteral("a")));
        Symbol symbol = predicate.normalizeSymbol(isNull, stmtCtx);
        assertThat(symbol, isLiteral(false));
    }

    @Test
    public void testNormalizeSymbolTrue() throws Exception {
        Function isNull = new Function(predicate.info(), Arrays.<Symbol>asList(Literal.NULL));
        Symbol symbol = predicate.normalizeSymbol(isNull, stmtCtx);
        assertThat(symbol, isLiteral(true));
    }

    @Test
    public void testNormalizeReference() throws Exception {
        Reference name_ref = new Reference(
                new ReferenceIdent(new TableIdent(null, "dummy"), "name"),
                RowGranularity.DOC,
                DataTypes.STRING);
        Function isNull = new Function(predicate.info(), Arrays.<Symbol>asList(name_ref));
        Symbol symbol = predicate.normalizeSymbol(isNull, stmtCtx);
        assertThat(symbol, instanceOf(Function.class));
    }

    @Test
    public void testNormalizeDynamicReference() throws Exception {
        DynamicReference name_ref = new DynamicReference(
            new ReferenceIdent(new TableIdent(null, "dummy"), "name"), RowGranularity.DOC);
        Function isNull = new Function(predicate.info(), Arrays.<Symbol>asList(name_ref));
        Symbol symbol = predicate.normalizeSymbol(isNull, stmtCtx);
        assertThat(symbol, isLiteral(true));
    }

    @Test
    public void testNormalizeSymbolWithStringLiteralThatIsNull() throws Exception {
        Function isNull = new Function(predicate.info(),
                Arrays.<Symbol>asList(Literal.newLiteral(DataTypes.STRING, null)));
        Symbol symbol = predicate.normalizeSymbol(isNull, stmtCtx);
        assertThat((Boolean) ((Input) symbol).value(), is(true));
    }

    @Test
    public void testEvaluateWithInputThatReturnsNull() throws Exception {
        Boolean result = predicate.evaluate(new Input[]{new Input() {
            @Override
            public Object value() {
                return null;
            }
        }});
        assertThat(result, is(true));
    }
}

