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

package io.crate.operation.projectors;

import com.google.common.collect.ImmutableList;
import io.crate.analyze.symbol.Literal;
import io.crate.core.collections.ArrayRow;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.collect.InputCollectExpression;
import io.crate.operation.projectors.sorting.OrderingByPosition;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.CollectingRowReceiver;
import org.junit.Test;

import static io.crate.testing.TestingHelpers.isRow;
import static org.hamcrest.core.Is.is;

public class SortingProjectorTest extends CrateUnitTest {

    private final ArrayRow spare = new ArrayRow();

    private Row spare(Object... cells) {
        if (cells == null) {
            cells = new Object[]{null};
        }
        spare.cells(cells);
        return spare;
    }

    private SortingProjector createProjector(int numOutputs, int offset, RowReceiver rowReceiver) {
        InputCollectExpression input = new InputCollectExpression(0);
        SortingProjector projector = new SortingProjector(
            ImmutableList.of(input, Literal.newLiteral(true)),
            ImmutableList.<CollectExpression<Row, ?>>of(input),
            numOutputs,
            OrderingByPosition.arrayOrdering(0, false, null),
            offset
        );
        projector.downstream(rowReceiver);
        return projector;
    }

    private void addRows(Projector projector) {
        int i;
        for (i = 10; i > 0; i--) {   // 10 --> 1
            if (!projector.setNextRow(spare(i))) {
                break;
            }
        }
        assertThat(i, is(0)); // needs to collect all it can get
    }

    @Test
    public void testOrderBy() throws Exception {
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        SortingProjector projector = createProjector(2, 0, rowReceiver);
        addRows(projector);
        projector.finish();
        Bucket rows = rowReceiver.result();
        assertThat(rows.size(), is(10));
        int iterateLength = 1;
        for (Row row : rows) {
            assertThat(row, isRow(iterateLength++, true));
        }
    }

    @Test
    public void testOrderByWithOffset() throws Exception {
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        SortingProjector projector = createProjector(2, 5, rowReceiver);
        addRows(projector);
        projector.finish();
        Bucket rows = rowReceiver.result();
        assertThat(rows.size(), is(5));
        int iterateLength = 6;
        for (Row row : rows) {
            assertThat(row, isRow(iterateLength++, true));
        }
    }

    @Test
    public void testInvalidOffset() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("invalid offset -1");

        new SortingTopNProjector(null, null, 2, null, 1, -1);
    }

}
