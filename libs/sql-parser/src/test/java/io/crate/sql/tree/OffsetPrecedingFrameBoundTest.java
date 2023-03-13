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


import static io.crate.sql.tree.FrameBound.Type.PRECEDING;
import static io.crate.sql.tree.WindowFrame.Mode.RANGE;
import static io.crate.sql.tree.WindowFrame.Mode.ROWS;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Comparator;
import java.util.List;

import org.junit.jupiter.api.Test;

class OffsetPrecedingFrameBoundTest {

    private static final Comparator<Integer> INT_COMPARATOR = Comparator.comparing(x -> x);
    private static final List<Integer> PARTITIONS = List.of(1, 2, 3, 6, 7);

    @Test
    void test_preceding_start_in_range_mode() {
        int frameStart = PRECEDING.getStart(RANGE, 0, 4, 4, 2L, 4, INT_COMPARATOR, PARTITIONS);
        assertThat(frameStart).isEqualTo(3);
    }

    @Test
    void test_preceding_start_in_rows_mode() {
        int frameStart = PRECEDING.getStart(ROWS, 0, 4, 3, 2L, null, INT_COMPARATOR, PARTITIONS);
        assertThat(frameStart).isEqualTo(1);
    }
}
