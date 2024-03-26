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

package io.crate.planner.operators;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.Map;

import org.junit.Test;

import io.crate.expression.symbol.AliasSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class MapBackedSymbolReplacerTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_convert_symbol_without_traversing() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (x int)");

        Symbol x = e.asSymbol("x");
        Symbol x_source_lookup = e.asSymbol("_doc['x']");
        Symbol alias = new AliasSymbol("a_alias", x);
        Symbol alias_source_lookup = new AliasSymbol("a_alias", x_source_lookup);

        // Mapping contains the requested symbol directly, must not traverse alias symbols
        Map<Symbol, Symbol> mapping = Map.of(
            alias,
            alias_source_lookup
        );

        assertThat(MapBackedSymbolReplacer.convert(alias, mapping), is(alias_source_lookup));
    }

    @Test
    public void test_convert_symbol_by_traversing() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table tbl (x int)");

        Symbol x = e.asSymbol("x");
        Symbol x_source_lookup = e.asSymbol("_doc['x']");
        Symbol alias = new AliasSymbol("a_alias", x);
        Symbol alias_source_lookup = new AliasSymbol("a_alias", x_source_lookup);

        // Mapping does not contain the requested symbol, but a child of it. Fallback to symbol traversing.
        Map<Symbol, Symbol> mapping = Map.of(
            x,
            x_source_lookup
        );

        assertThat(MapBackedSymbolReplacer.convert(alias, mapping), is(alias_source_lookup));
    }
}
