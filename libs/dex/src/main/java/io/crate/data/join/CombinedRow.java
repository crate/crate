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

package io.crate.data.join;

import io.crate.data.Row;

public class CombinedRow extends Row implements ElementCombiner<Row, Row, Row> {

    private final int numCols;
    private final int leftNumCols;
    private Row left;
    private Row right;

    public CombinedRow(int leftNumCols, int rightNumCols) {
        this.leftNumCols = leftNumCols;
        this.numCols = leftNumCols + rightNumCols;
    }

    @Override
    public int numColumns() {
        return numCols;
    }

    @Override
    public Object get(int index) {
        if (index < leftNumCols) {
            if (left == null) {
                return null;
            }
            return left.get(index);
        }
        index = index - leftNumCols;
        if (right == null) {
            return null;
        }
        return right.get(index);
    }

    @Override
    public Row currentElement() {
        return this;
    }

    public void nullLeft() {
        left = null;
    }

    public void nullRight() {
        right = null;
    }

    @Override
    public void setRight(Row o) {
        right = o;
    }

    @Override
    public void setLeft(Row o) {
        left = o;
    }

    @Override
    public String toString() {
        return "CombinedRow{" +
               "lhs=" + left +
               ", rhs=" + right +
               '}';
    }
}
