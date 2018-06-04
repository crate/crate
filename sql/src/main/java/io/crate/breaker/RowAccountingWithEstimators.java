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

package io.crate.breaker;

import io.crate.data.Row;
import io.crate.execution.engine.join.HashInnerJoinBatchIterator;
import io.crate.types.DataType;

import java.util.ArrayList;
import java.util.Collection;

public class RowAccountingWithEstimators implements RowAccounting {

    private final RamAccountingContext ramAccountingContext;
    private final ArrayList<SizeEstimator<Object>> estimators;
    private int extraSizePerRow = 0;

    /**
     * See {@link RowAccountingWithEstimators#RowAccountingWithEstimators(Collection, RamAccountingContext, int)}
     */
    public RowAccountingWithEstimators(Collection<? extends DataType> columnTypes, RamAccountingContext ramAccountingContext) {
        this.estimators = new ArrayList<>(columnTypes.size());
        for (DataType columnType : columnTypes) {
            estimators.add(SizeEstimatorFactory.create(columnType));
        }
        this.ramAccountingContext = ramAccountingContext;
    }

    /**
     * @param columnTypes           Column types are needed to use the correct {@link SizeEstimator} per column
     * @param ramAccountingContext  {@link RamAccountingContext} implementing the CircuitBreaker logic
     * @param extraSizePerRow       Extra size that need to be calculated per row. E.g. {@link HashInnerJoinBatchIterator}
     *                              might instantiate an ArrayList per row used for the internal hash->row buffer
     */
    public RowAccountingWithEstimators(Collection<? extends DataType> columnTypes,
                                       RamAccountingContext ramAccountingContext,
                                       int extraSizePerRow) {
        this(columnTypes, ramAccountingContext);
        this.extraSizePerRow = extraSizePerRow;
    }

    /**
     * Account for the size of the values of the row.
     *
     * This should only be used if the values are stored/buffered in another in-memory data structure.
     */
    @Override
    public void accountForAndMaybeBreak(Row row) {
        assert row.numColumns() == estimators.size() : "Size of row must match the number of estimators";

        // Container size of the row is excluded because here it's unknown where the values will be saved to.
        // As size estimation is generally "best-effort" this should be good enough.
        long size = 0;
        for (int i = 0; i < row.numColumns(); i++) {
            size += (estimators.get(i).estimateSize(row.get(i)) + extraSizePerRow);
        }
        ramAccountingContext.addBytes(size);
    }

    @Override
    public void release() {
        ramAccountingContext.release();
    }

    public void close() {
        ramAccountingContext.close();
    }

}
