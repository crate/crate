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

import io.crate.data.Row;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.ArrayList;
import java.util.function.ToLongFunction;

public class TypeGuessEstimateRowSize implements ToLongFunction<Row> {

    private EstimateRowSize estimateRowSize = null;

    @Override
    public long applyAsLong(Row row) {
        if (estimateRowSize == null) {
            ArrayList<DataType<?>> guessedTypes = new ArrayList<>(row.numColumns());
            for (int i = 0; i < row.numColumns(); i++) {
                guessedTypes.add(DataTypes.guessType(row.get(i)));
            }
            estimateRowSize = new EstimateRowSize(guessedTypes);
        }
        return estimateRowSize.applyAsLong(row);
    }
}
