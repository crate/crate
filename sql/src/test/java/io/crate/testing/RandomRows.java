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

package io.crate.testing;

import com.google.common.collect.AbstractIterator;
import io.crate.core.collections.Row;
import io.crate.core.collections.RowN;
import org.apache.commons.lang3.RandomUtils;

import java.util.Iterator;

public class RandomRows implements Iterable<Row> {

    private final int numRows;
    private final Object[] cells;
    private final Row row;
    private int rowsProduced = 0;
    private int currentValue;
    private final int sameValues;

    public RandomRows(int numRows) {
        this(numRows, RandomUtils.nextInt(1, 5), RandomUtils.nextInt(0, 10));
    }

    public RandomRows(int numRows, int sameValues, int startValue) {
        this.numRows = numRows;
        this.cells = new Object[1];
        this.row = new RowN(this.cells);
        this.sameValues = sameValues;
        this.currentValue = startValue;
    }

    @Override
    public Iterator<Row> iterator() {
        return new AbstractIterator<Row>() {
            @Override
            protected Row computeNext() {
                if (rowsProduced >= numRows) {
                    return endOfData();
                }
                if (rowsProduced % sameValues == 0) {
                    currentValue++;
                    cells[0] = currentValue;
                }
                rowsProduced++;
                return row;
            }
        };
    }
}
