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

package io.crate.data;

/**
 * A {@link ArrayRow} variant which uses 2 backing arrays which can be changed.
 */
public class BiArrayRow extends Row {

    private Object[] firstCells;
    private Object[] secondCells;

    public BiArrayRow(Object[] firstCells, Object[] secondCells) {
        this.firstCells = firstCells;
        this.secondCells = secondCells;
    }

    @Override
    public int numColumns() {
        return firstCells.length + secondCells.length;
    }

    @Override
    public Object get(int index) {
        int firstCellsSize = firstCells.length;
        if (index >= firstCellsSize) {
            return secondCells[index - firstCellsSize];
        }
        return firstCells[index];
    }

    @Override
    public Object[] materialize() {
        Object[] copy = new Object[numColumns()];
        System.arraycopy(firstCells, 0, copy, 0, firstCells.length);
        System.arraycopy(secondCells, 0, copy, firstCells.length, secondCells.length);
        return copy;
    }
}
