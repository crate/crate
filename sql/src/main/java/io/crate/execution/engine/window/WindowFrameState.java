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

import javax.annotation.Nullable;
import java.util.List;

/**
 * Holds the runtime information of a window frame during the execution of a window function.
 */
public final class WindowFrameState {

    private final int lowerBound;
    private final int upperBoundExclusive;
    private List<Object[]> rows;

    WindowFrameState(int lowerBound, int upperBoundExclusive, List<Object[]> rows) {
        this.lowerBound = lowerBound;
        this.upperBoundExclusive = upperBoundExclusive;
        this.rows = rows;
    }

    int lowerBound() {
        return lowerBound;
    }

    public int upperBoundExclusive() {
        return upperBoundExclusive;
    }

    public Iterable<Object[]> getRows() {
        return rows;
    }

    /**
     * Returns the number of rows that are part of this frame.
     */
    public int size() {
        return rows.size();
    }

    /**
     * Return the row at the given index in the frame or null if the index is out of bounds.
     */
    @Nullable
    public Object[] getRowAtIndexOrNull(int index) {
        if (index < 0 || index >= rows.size()) {
            return null;
        }

        return rows.get(index);
    }
}
