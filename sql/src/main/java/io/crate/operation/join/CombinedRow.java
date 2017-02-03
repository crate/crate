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

package io.crate.operation.join;

import io.crate.data.Row;

class CombinedRow implements Row {

    volatile Row outerRow;
    volatile Row innerRow;

    @Override
    public int numColumns() {
        return outerRow.numColumns() + innerRow.numColumns();
    }

    @Override
    public Object get(int index) {
        if (index < outerRow.numColumns()) {
            return outerRow.get(index);
        }
        return innerRow.get(index - outerRow.numColumns());
    }

    @Override
    public Object[] materialize() {
        Object[] left = outerRow.materialize();
        Object[] right = innerRow.materialize();

        Object[] newRow = new Object[left.length + right.length];
        System.arraycopy(left, 0, newRow, 0, left.length);
        System.arraycopy(right, 0, newRow, left.length, right.length);
        return newRow;
    }

    @Override
    public String toString() {
        return "CombinedRow{" +
               " outer=" + outerRow +
               ", inner=" + innerRow +
               '}';
    }
}
