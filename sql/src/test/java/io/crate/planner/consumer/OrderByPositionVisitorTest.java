/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import com.google.common.collect.ImmutableList;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataTypes;
import org.junit.Test;

public class OrderByPositionVisitorTest extends CrateUnitTest {
    @Test
    public void testOrderByPositionInputs() throws Exception {
        int[] orderByPositions = OrderByPositionVisitor.orderByPositions(
                ImmutableList.<Symbol>of(new InputColumn(0), new InputColumn(1), new InputColumn(0)),
                ImmutableList.<Symbol>of(Literal.BOOLEAN_TRUE, Literal.newLiteral(1))
        );
        assertArrayEquals(new int[]{0, 1, 0}, orderByPositions);
    }

    @Test
    public void testSymbols() throws Exception {
        Reference ref = TestingHelpers.createReference("column", DataTypes.STRING);
        int[] orderByPositions = OrderByPositionVisitor.orderByPositions(
                ImmutableList.of(ref, new InputColumn(1), new InputColumn(0)),
                ImmutableList.of(ref, Literal.newLiteral(1))
        );
        assertArrayEquals(new int[]{0, 1, 0}, orderByPositions);
    }

    @Test
    public void testSymbolNotContained() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot sort by: dummyTable.other - not part of source symbols");

        Reference ref = TestingHelpers.createReference("column", DataTypes.STRING);
        OrderByPositionVisitor.orderByPositions(
                ImmutableList.of(ref, new InputColumn(1), TestingHelpers.createReference("other", DataTypes.LONG)),
                ImmutableList.of(ref, Literal.BOOLEAN_FALSE)
        );

    }
}