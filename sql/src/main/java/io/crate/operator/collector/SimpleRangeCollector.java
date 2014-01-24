/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operator.collector;

import io.crate.operator.Input;
import io.crate.operator.RowCollector;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A collector which returns a range given range of a collected inputs
 */

public class SimpleRangeCollector implements RowCollector<Object[][]> {

    private final AtomicInteger collected = new AtomicInteger();
    private int endPos;
    private final Object endPosMutex = new Object();


    private final Input<Object[]> input;
    private Object[][] result;
    private final int start;
    private final int end;

    /**
     * Creates a new range collector
     *
     * @param offset the offset where the range starts
     * @param limit  the size of the range
     * @param input  the input implementation to get the values from
     */
    public SimpleRangeCollector(int offset, int limit, Input<Object[]> input) {
        this.start = offset;
        this.end = start + limit;
        this.result = new Object[end - start][];
        this.input = input;
    }

    @Override
    public boolean startCollect() {
        endPos = 0;
        collected.set(0);
        return true;
    }

    @Override
    public boolean processRow() {
        int pos = collected.incrementAndGet() - 1;
        if (pos > end) {
            return false;
        } else if (pos < start) {
            return true;
        }
        if (pos != end) {
            int arrayPos = pos - start;
            result[arrayPos] = input.value();
            synchronized (endPosMutex) {
                endPos = Math.max(endPos, arrayPos);
            }
        }
        return true;
    }

    @Override
    public Object[][] finishCollect() {
        if (result.length == endPos + 1) {
            return result;
        }
        return Arrays.copyOf(result, endPos + 1);
    }
}
