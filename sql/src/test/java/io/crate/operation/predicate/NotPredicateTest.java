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

import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.*;
import io.crate.operation.operator.EqOperator;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.junit.Test;

import java.util.Arrays;

import static io.crate.testing.TestingHelpers.isLiteral;
import static org.hamcrest.CoreMatchers.instanceOf;

public class NotPredicateTest extends CrateUnitTest {

    private final StmtCtx stmtCtx = new StmtCtx();

    @Test
    public void testNormalizeSymbolBoolean() throws Exception {
        NotPredicate predicate = new NotPredicate();
        Function not = new Function(predicate.info(), Arrays.<Symbol>asList(Literal.of(true)));

        assertThat(predicate.normalizeSymbol(not, stmtCtx), isLiteral(false));
    }

    @Test
    public void testNormalizeSymbol() throws Exception {
        NotPredicate notPredicate = new NotPredicate();

        Reference name_ref = new Reference(
            new ReferenceIdent(new TableIdent(null, "dummy"), "foo"),
            RowGranularity.DOC, DataTypes.STRING);
        Function eqName = new Function(
            new FunctionInfo(
                new FunctionIdent(EqOperator.NAME,
                    Arrays.<DataType>asList(DataTypes.STRING, DataTypes.STRING)),
                DataTypes.BOOLEAN
            ),
            Arrays.<Symbol>asList(name_ref, Literal.of("foo"))
        );

        Function not = new Function(notPredicate.info(), Arrays.<Symbol>asList(eqName));
        Symbol normalized = notPredicate.normalizeSymbol(not, stmtCtx);

        assertThat(normalized, instanceOf(Function.class));
    }
}
