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

package io.crate.planner.consumer;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Reference;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataTypes;

public class OrderByPositionVisitorTest extends ESTestCase {

    @Test
    public void testOrderByPositionInputs() throws Exception {
        int[] orderByPositions = OrderByPositionVisitor.orderByPositions(
            List.<Symbol>of(new InputColumn(0), new InputColumn(1), new InputColumn(0)),
            List.<Symbol>of(Literal.BOOLEAN_TRUE, Literal.of(1))
        );
        assertThat(orderByPositions).isEqualTo(new int[]{0, 1, 0});
    }

    @Test
    public void testSymbols() throws Exception {
        Reference ref = TestingHelpers.createReference("column", DataTypes.STRING);
        int[] orderByPositions = OrderByPositionVisitor.orderByPositions(
            List.of(ref, new InputColumn(1), new InputColumn(0)),
            List.of(ref, Literal.of(1))
        );
        assertThat(orderByPositions).isEqualTo(new int[]{0, 1, 0});
    }
}
