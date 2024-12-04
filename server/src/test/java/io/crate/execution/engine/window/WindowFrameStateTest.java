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

package io.crate.execution.engine.window;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.Test;

public class WindowFrameStateTest {

    private final List<Object[]> rows = List.of(
        new Object[]{1},
        new Object[]{2},
        new Object[]{3},
        new Object[]{4},
        new Object[]{5},
        new Object[]{6});

    private final WindowFrameState wfs = new WindowFrameState(0, rows.size(), rows);

    @Test
    public void testGetRowWithinRangeInSinglePartition() {
        wfs.updateBounds(0, 6, 0, 6);
        assertThat(wfs.getRowInPartitionAtIndexOrNull(1)).isEqualTo(rows.get(1));
    }

    @Test
    public void testGetRowOutOfRangeInSinglePartition() {
        wfs.updateBounds(0, 6, 0, 6);
        assertThat(wfs.getRowInPartitionAtIndexOrNull(-1)).isNull();
        assertThat(wfs.getRowInPartitionAtIndexOrNull(rows.size())).isNull();
    }

    @Test
    public void testGetRowsWithinRangeInTwoPartitionReturnsNull() {
        // Partition 1 index range: [0, 3); frame [0, 1)
        wfs.updateBounds(0, 3, 0, 1);
        assertThat(wfs.getRowInPartitionAtIndexOrNull(0)).isEqualTo(rows.get(0));
        assertThat(wfs.getRowInPartitionAtIndexOrNull(2)).isEqualTo(rows.get(2));

        // Partition 2 index range: [3, 6); frame [3, 6)
        wfs.updateBounds(3, 6, 3, 6);
        assertThat(wfs.getRowInPartitionAtIndexOrNull(0)).isEqualTo(rows.get(3));
        assertThat(wfs.getRowInPartitionAtIndexOrNull(2)).isEqualTo(rows.get(5));
    }

    @Test
    public void testGetRowsOutOfRangeInTwoPartitionReturnsNull() {
        // Partition 1 index range: [0, 3); frame [0, 1)
        wfs.updateBounds(0, 3, 0, 1);
        assertThat(wfs.getRowInPartitionAtIndexOrNull(-1)).isNull();
        assertThat(wfs.getRowInPartitionAtIndexOrNull(3)).isNull();

        // Partition 2 index range: [3, 6); frame [3, 6)
        wfs.updateBounds(3, 6, 3, 6);
        assertThat(wfs.getRowInPartitionAtIndexOrNull(-1)).isNull();
        assertThat(wfs.getRowInPartitionAtIndexOrNull(3)).isNull();
    }
}
