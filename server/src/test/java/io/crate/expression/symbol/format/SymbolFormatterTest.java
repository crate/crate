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

package io.crate.expression.symbol.format;

import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.functions.Signature;
import org.elasticsearch.test.ESTestCase;
import io.crate.types.DataTypes;
import org.junit.Test;

import java.util.IllegalFormatConversionException;
import java.util.List;

import static org.hamcrest.Matchers.is;

public class SymbolFormatterTest extends ESTestCase {

    @Test
    public void testFormat() throws Exception {
        Function f = new Function(
            Signature.scalar(
                "foo",
                DataTypes.STRING.getTypeSignature(),
                DataTypes.UNDEFINED.getTypeSignature(),
                DataTypes.DOUBLE.getTypeSignature()
            ),
            List.of(Literal.of("bar"), Literal.of(3.4)),
            DataTypes.DOUBLE
        );
        assertThat(Symbols.format("This Symbol is formatted %s", f), is("This Symbol is formatted foo('bar', 3.4)"));
    }

    @Test
    public void testFormatInvalidEscape() throws Exception {
        expectedException.expect(IllegalFormatConversionException.class);
        expectedException.expectMessage("d != java.lang.String");
        assertThat(Symbols.format("%d", Literal.of(42L)), is(""));
    }
}
