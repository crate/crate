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

package io.crate.breaker;

import java.util.List;

import io.crate.data.breaker.RamAccounting;
import io.crate.data.breaker.RowAccounting;
import io.crate.execution.engine.join.HashInnerJoinBatchIterator;
import io.crate.types.DataType;

public class RowCellsAccountingWithEstimators implements RowAccounting<Object[]> {

    private final RamAccounting ramAccounting;
    private final int extraSizePerRow;
    private final List<? extends DataType> columnTypes;

    /**
     * @param columnTypes     Column types are needed to use the correct {@link SizeEstimator} per column
     * @param ramAccounting   {@link RamAccounting} implementing the CircuitBreaker logic
     * @param extraSizePerRow Extra size that need to be calculated per row. E.g. {@link HashInnerJoinBatchIterator}
     *                        might instantiate an ArrayList per row used for the internal hash->row buffer
     */
    public RowCellsAccountingWithEstimators(List<? extends DataType> columnTypes,
                                            RamAccounting ramAccounting,
                                            int extraSizePerRow) {
        this.columnTypes = columnTypes;
        this.ramAccounting = ramAccounting;
        this.extraSizePerRow = extraSizePerRow;
    }

    /**
     * Account for the size of the values of the row cells (this can include all the output cells, not just the source
     * row).
     * <p>
     * This should only be used if the values are stored/buffered in another in-memory data structure.
     */
    @Override
    public long accountForAndMaybeBreak(Object[] rowCells) {
        long rowBytes = accountRowBytes(rowCells);
        ramAccounting.addBytes(rowBytes);
        return rowBytes;
    }

    public long accountRowBytes(Object[] rowCells) {
        assert rowCells.length == columnTypes.size() : "Size of row must match the number of estimators";
        long size = 0;
        for (int i = 0; i < rowCells.length; i++) {
            DataType dataType = columnTypes.get(i);
            size += (dataType.valueBytes(rowCells[i]) + extraSizePerRow);
        }
        return size;
    }

    @Override
    public void release() {
        ramAccounting.release();
    }
}
