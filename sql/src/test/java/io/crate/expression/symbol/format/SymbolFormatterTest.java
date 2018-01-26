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

package io.crate.expression.symbol.format;

import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.junit.Test;

import java.util.Arrays;
import java.util.IllegalFormatConversionException;

import static org.hamcrest.Matchers.is;

public class SymbolFormatterTest extends CrateUnitTest {

    @Test
    public void testFormat() throws Exception {
        Function f = new Function(new FunctionInfo(
            new FunctionIdent("foo", Arrays.<DataType>asList(DataTypes.STRING, DataTypes.UNDEFINED)), DataTypes.DOUBLE),
            Arrays.<Symbol>asList(Literal.of("bar"), Literal.of(3.4)));
        assertThat(SymbolFormatter.format("This Symbol is formatted %s", f), is("This Symbol is formatted foo('bar', 3.4)"));
    }

    @Test
    public void testFormatInvalidEscape() throws Exception {
        expectedException.expect(IllegalFormatConversionException.class);
        expectedException.expectMessage("d != java.lang.String");
        assertThat(SymbolFormatter.format("%d", Literal.of(42L)), is(""));
    }
}
