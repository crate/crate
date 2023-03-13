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

import static io.crate.sql.tree.FrameBound.Type.FOLLOWING;
import static io.crate.sql.tree.WindowFrame.Mode.RANGE;
import static io.crate.sql.tree.WindowFrame.Mode.ROWS;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Comparator;
import java.util.List;

import org.junit.jupiter.api.Test;

class OffsetFollowingFrameBoundTest {

    private static final List<Integer> PARTITIONS = List.of(1, 2, 3, 6, 7);
    private static final Comparator<Integer> INT_COMPARATOR = Comparator.comparing(x -> x);

    @Test
    public void test_following_end_in_range_mode() {
        int frameStart = FOLLOWING.getEnd(RANGE, 1, 5, 1, 2, 4, INT_COMPARATOR, PARTITIONS);
        assertThat(frameStart).isEqualTo(3);
    }

    @Test
    public void test_following_end_in_rows_mode() {
        int frameStart = FOLLOWING.getEnd(ROWS, 1, 5, 1, 2L, null, INT_COMPARATOR, PARTITIONS);
        assertThat(frameStart).isEqualTo(4);
    }
}
