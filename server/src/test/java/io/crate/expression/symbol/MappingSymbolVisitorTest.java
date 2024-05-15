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

import java.util.Map;

import org.junit.Test;

public class MappingSymbolVisitorTest {

    @Test
    public void testProcess() throws Exception {
        Symbol f = Literal.BOOLEAN_FALSE;
        Symbol t = Literal.BOOLEAN_TRUE;
        Symbol one = Literal.of(1);

        Map<Symbol, ? extends Symbol> fieldMap = Map.of(f, t);

        assertThat(MappingSymbolVisitor.copy().process(f, fieldMap)).isEqualTo(t);
        assertThat(MappingSymbolVisitor.copy().process(one, fieldMap)).isEqualTo(one);
    }
}
