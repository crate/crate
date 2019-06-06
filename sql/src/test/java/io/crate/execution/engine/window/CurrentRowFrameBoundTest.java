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

package io.crate.execution.engine.window;

import io.crate.test.integration.CrateUnitTest;
import org.junit.Before;
import org.junit.Test;

import java.util.Comparator;
import java.util.List;

import static io.crate.sql.tree.FrameBound.Type.CURRENT_ROW;
import static org.hamcrest.core.Is.is;

public class CurrentRowFrameBoundTest extends CrateUnitTest {

    private List<Integer> partition;
    private Comparator<Integer> intComparator;

    @Before
    public void setupPartitionAndComparator() {
        intComparator = Comparator.comparing(x -> x);
        partition = List.of(1, 1, 2);
    }

    @Test
    public void testCurrentRowEndIsFirstNonPeer() {
        int firstFrameEnd = CURRENT_ROW.getEnd(0, 3, 0, intComparator, partition);
        assertThat(firstFrameEnd, is(2));
        int secondFrameEnd = CURRENT_ROW.getEnd(0, 3, 1, intComparator, partition);
        assertThat(secondFrameEnd, is(2));
    }

    @Test
    public void testCurrentRowStartForOrderedPartition() {
        int firstFrameStart = CURRENT_ROW.getStart(0, 3, 0, intComparator, partition);
        assertThat(firstFrameStart, is(0));
        int secondFrameStart = CURRENT_ROW.getStart(0, 3, 1, intComparator, partition);
        assertThat("a new frame starts when encountering a non-peer", secondFrameStart, is(0));
        int thirdFrameStart = CURRENT_ROW.getStart(0, 3, 2, intComparator, partition);
        assertThat(thirdFrameStart, is(2));
    }

    @Test
    public void testCurrentRowStartIsTheRowIdForUnorderedPartitions() {
        int firstFrameStart = CURRENT_ROW.getStart(0, 3, 0, intComparator, partition);
        assertThat(firstFrameStart, is(0));
        int secondFrameStart = CURRENT_ROW.getStart(0, 3, 1, intComparator, partition);
        assertThat(secondFrameStart, is(0));
        int thirdFrameStart = CURRENT_ROW.getStart(0, 3, 2, intComparator, partition);
        assertThat(thirdFrameStart, is(2));
    }

    @Test
    public void testCurrentRowStartForPeersThatSpanMultiplePartitions() {
        var window = List.of(1, 2, 2, 2, 2, 2, 2, 2, 3);
        int frameStartForFourthRow = CURRENT_ROW.getStart(1, 4, 3, intComparator, window);
        assertThat(frameStartForFourthRow, is(1));
        int frameStartForSixthRow = CURRENT_ROW.getStart(4, 7, 5, intComparator, window);
        assertThat("frame start shouldn't be outside of the partition bounds", frameStartForSixthRow, is(4));
    }

}
