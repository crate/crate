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

package io.crate.sql.tree;


import static io.crate.sql.tree.FrameBound.Type.CURRENT_ROW;
import static io.crate.sql.tree.WindowFrame.Mode.RANGE;
import static io.crate.sql.tree.WindowFrame.Mode.ROWS;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Comparator;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CurrentRowFrameBoundTest {

    private List<Integer> partition;
    private Comparator<Integer> intComparator;

    @BeforeEach
    public void setupPartitionAndComparator() {
        intComparator = Comparator.comparing(x -> x);
        partition = List.of(1, 1, 2);
    }

    @Test
    public void test_current_row_end_in_range_mode_is_first_non_peer() {
        int firstFrameEnd = CURRENT_ROW.getEnd(RANGE, 0, 3, 0, null, null, intComparator, partition);
        assertThat(firstFrameEnd).isEqualTo(2);
        int secondFrameEnd = CURRENT_ROW.getEnd(RANGE, 0, 3, 1, null, null, intComparator, partition);
        assertThat(secondFrameEnd).isEqualTo(2);
    }

    @Test
    public void test_current_row_end_in_rows_mode_is_row_index() {
        int firstFrameEnd = CURRENT_ROW.getEnd(ROWS, 0, 3, 0, null, null, intComparator, partition);
        assertThat(firstFrameEnd).isEqualTo(1);
        int secondFrameEnd = CURRENT_ROW.getEnd(ROWS, 0, 3, 1, null, null, intComparator, partition);
        assertThat(secondFrameEnd).isEqualTo(2);
    }

    @Test
    public void test_current_row_start_in_ordered_range_mode_is_first_peer_index() {
        int firstFrameStart = CURRENT_ROW.getStart(RANGE, 0, 3, 0, null, null, intComparator, partition);
        assertThat(firstFrameStart).isEqualTo(0);
        int secondFrameStart = CURRENT_ROW.getStart(RANGE, 0, 3, 1, null, null, intComparator, partition);
        assertThat(secondFrameStart).as("a new frame starts when encountering a non-peer").isEqualTo(0);
        int thirdFrameStart = CURRENT_ROW.getStart(RANGE, 0, 3, 2, null, null, intComparator, partition);
        assertThat(thirdFrameStart).isEqualTo(2);
    }

    @Test
    public void test_current_row_start_in_ordered_row_is_row_index() {
        int firstFrameStart = CURRENT_ROW.getStart(ROWS, 0, 3, 0, null, null, intComparator, partition);
        assertThat(firstFrameStart).isEqualTo(0);
        int secondFrameStart = CURRENT_ROW.getStart(ROWS, 0, 3, 1, null, null, intComparator, partition);
        assertThat(secondFrameStart).isEqualTo(1);
        int thirdFrameStart = CURRENT_ROW.getStart(ROWS, 0, 3, 2, null, null, intComparator, partition);
        assertThat(thirdFrameStart).isEqualTo(2);
    }

    @Test
    public void test_current_row_start_in_range_mode_unordered_partition_is_row_index() {
        int firstFrameStart = CURRENT_ROW.getStart(RANGE, 0, 3, 0, null, null, intComparator, partition);
        assertThat(firstFrameStart).isEqualTo(0);
        int secondFrameStart = CURRENT_ROW.getStart(RANGE, 0, 3, 1, null, null, intComparator, partition);
        assertThat(secondFrameStart).isEqualTo(0);
        int thirdFrameStart = CURRENT_ROW.getStart(RANGE, 0, 3, 2, null, null, intComparator, partition);
        assertThat(thirdFrameStart).isEqualTo(2);
    }

    @Test
    public void test_current_row_start_range_mode_for_peers_crossing_partitions() {
        var window = List.of(1, 2, 2, 2, 2, 2, 2, 2, 3);
        int frameStartForFourthRow = CURRENT_ROW.getStart(RANGE, 1, 4, 3, null, null, intComparator, window);
        assertThat(frameStartForFourthRow).isEqualTo(1);
        int frameStartForSixthRow = CURRENT_ROW.getStart(RANGE, 4, 7, 5, null, null, intComparator, window);
        assertThat(frameStartForSixthRow).as("frame start shouldn't be outside of the partition bounds").isEqualTo(4);
    }
}
