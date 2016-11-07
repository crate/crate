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

package io.crate.core.collections;

/**
 * Used to wrap a {@link Row} and prepend inputId as the first column
 */
public class PrependedRow implements Row {

    private final byte inputId;
    private Row delegateRow;

    /**
     * Before using this special row {@link #setDelegate(Row)} must be called first.
     */
    public PrependedRow(byte inputId) {
        this.inputId = inputId;
    }

    public void setDelegate(Row row) {
        delegateRow = row;
    }

    @Override
    public int size() {
        assert delegateRow != null : "delegateRow must be set first";
        return delegateRow.size() + 1;
    }

    @Override
    public Object get(int index) {
        assert delegateRow != null : "delegateRow must be set first";
        return index == 0 ? inputId : delegateRow.get(index - 1);
    }

    @Override
    public Object[] materialize() {
        Object[] materializedRow = new Object[size()];
        materializedRow[0] = inputId;
        System.arraycopy(delegateRow.materialize(), 0, materializedRow, 1, delegateRow.size());
        return materializedRow;
    }

    @Override
    public String toString() {
        return "PrependedRow{inputId=" + inputId + ", row=" + delegateRow.toString() + '}';
    }
}
