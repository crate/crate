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

package io.crate.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import io.crate.analyze.OrderBy;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Reference;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataTypes;

public class PositionalOrderByTest {

    @Test
    public void testNewOutputMapping() throws Exception {

        List<Symbol> oldOutputs = Arrays.asList(ref("a"), ref("b"), ref("c"), ref("d"));
        List<Symbol> newOutputs = Arrays.asList(ref("b"), ref("c"), ref("d"), ref("a"));

        OrderBy orderBy = new OrderBy(
            Arrays.asList(ref("a"), ref("c"), ref("b")),
            new boolean[]{true, false, true},
            new boolean[]{true, false, false}
        );
        PositionalOrderBy positionalOrderBy = PositionalOrderBy.of(orderBy, oldOutputs);
        PositionalOrderBy newOrderBy = positionalOrderBy.tryMapToNewOutputs(oldOutputs, newOutputs);

        PositionalOrderBy expected = PositionalOrderBy.of(orderBy, newOutputs);
        assertThat(newOrderBy.indices()).isEqualTo(expected.indices());
        assertThat(newOrderBy.reverseFlags()).isEqualTo(expected.reverseFlags());
        assertThat(newOrderBy.nullsFirst()).isEqualTo(expected.nullsFirst());
    }

    private static Reference ref(String name) {
        return TestingHelpers.createReference(name, DataTypes.LONG);
    }
}
