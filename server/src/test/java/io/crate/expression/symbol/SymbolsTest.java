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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Test;

import io.crate.metadata.Reference;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class SymbolsTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testNullableSerializationOfNull() throws IOException {
        BytesStreamOutput output = new BytesStreamOutput();
        Symbol.nullableToStream(null, output);

        StreamInput input = output.bytes().streamInput();
        Symbol symbol = Symbol.nullableFromStream(input);

        assertThat(symbol).isNull();
    }

    @Test
    public void testNullableSerialization() throws IOException {
        Literal<Long> inputSymbol = Literal.of(42L);
        BytesStreamOutput output = new BytesStreamOutput();
        Symbol.nullableToStream(inputSymbol, output);

        StreamInput inputStream = output.bytes().streamInput();
        Symbol deserialisedSymbol = Symbol.nullableFromStream(inputStream);

        assertThat(inputSymbol).isEqualTo(deserialisedSymbol);
        assertThat(inputSymbol.hashCode()).isEqualTo(deserialisedSymbol.hashCode());
    }


    @Test
    public void test_symbols_are_accountable() throws Exception {
        var e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (x int)");
        Symbol symbol = e.asSymbol("x + 10 > 100");
        assertThat(symbol.ramBytesUsed()).isEqualTo(912L);
    }

    @Test
    public void test_contains_considers_subscripts() throws Exception {
        var e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (y int, o object as (o1 object as (x int)))");
        Reference y = (Reference) e.asSymbol("y");
        Reference o = (Reference) e.asSymbol("o");
        Reference o1 = (Reference) e.asSymbol("o['o1']");
        Reference ox = (Reference) e.asSymbol("o['o1']['x']");

        assertThat(Symbols.contains(List.of(o), o)).isEqualTo(true);
        assertThat(Symbols.contains(List.of(o), ox)).isEqualTo(true);
        assertThat(Symbols.contains(List.of(o), o1)).isEqualTo(true);
        assertThat(Symbols.contains(List.of(o), y)).isEqualTo(false);
        assertThat(Symbols.contains(List.of(o1), ox)).isEqualTo(true);
    }
}
