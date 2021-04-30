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

package io.crate.expression.symbol;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class SymbolsTest {

    @Test
    public void testNullableSerializationOfNull() throws IOException {
        BytesStreamOutput output = new BytesStreamOutput();
        Symbols.nullableToStream(null, output);

        StreamInput input = output.bytes().streamInput();
        Symbol symbol = Symbols.nullableFromStream(input);

        assertThat(symbol, is(nullValue()));
    }

    @Test
    public void testNullableSerialization() throws IOException {
        Literal<Long> inputSymbol = Literal.of(42L);
        BytesStreamOutput output = new BytesStreamOutput();
        Symbols.nullableToStream(inputSymbol, output);

        StreamInput inputStream = output.bytes().streamInput();
        Symbol deserialisedSymbol = Symbols.nullableFromStream(inputStream);

        assertThat(inputSymbol, is(equalTo(deserialisedSymbol)));
        assertThat(inputSymbol.hashCode(), is(deserialisedSymbol.hashCode()));
    }

}
