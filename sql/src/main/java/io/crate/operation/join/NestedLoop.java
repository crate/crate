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

package io.crate.operation.join;

import com.google.common.collect.AbstractIterator;

import java.util.Iterator;

public class NestedLoop extends AbstractIterator<Object[]> implements Iterable<Object[]> {
    private final Object[][] left;
    private final Object[][] right;
    private int countDown;
    private int leftLen, rightLen;
    private int leftIdx = 0, rightIdx = 0;
    private final int leftRowLength;
    private final int rightRowLength;

    public NestedLoop(Object[][] left, Object[][] right, int limit) {
        this.left = left;
        this.leftLen = left.length;
        this.right = right;
        this.rightLen = right.length;

        this.countDown = Math.min(limit, leftLen*rightLen);
        if (leftLen > 0 && rightLen > 0) {
            leftRowLength = left[0].length;
            rightRowLength =  right[0].length;
        } else {
            leftRowLength = -1;
            rightRowLength = -1;
            endOfData(); // no results, computeNext is never called
        }
    }

    @Override
    protected Object[] computeNext() {
        if (countDown-- == 0 || leftIdx >= leftLen) {
            endOfData();
            return null;
        }
        if (rightIdx >= rightLen) {
            rightIdx = 0;
            leftIdx++;
            if (leftIdx >= leftLen) {
                endOfData();
                return null;
            }
        }
        return combine(left[leftIdx], right[rightIdx++]);

    }

    private Object[] combine(Object[] left, Object[] right) {
        // TODO: avoid creating new array for each row?
        Object[] newRow = new Object[leftRowLength + rightRowLength];
        System.arraycopy(left, 0, newRow, 0, leftRowLength);
        System.arraycopy(right, 0, newRow, leftRowLength, rightRowLength);
        return newRow;
    }

    @Override
    public Iterator<Object[]> iterator() {
        return this;
    }
}
