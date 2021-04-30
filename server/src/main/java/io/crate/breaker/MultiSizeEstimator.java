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

import io.crate.types.DataType;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class MultiSizeEstimator extends SizeEstimator<List<Object>> {

    private final List<SizeEstimator<Object>> subEstimators;

    public MultiSizeEstimator(List<? extends DataType> keyTypes) {
        subEstimators = new ArrayList<>(keyTypes.size());
        for (DataType<?> keyType : keyTypes) {
            subEstimators.add(SizeEstimatorFactory.create(keyType));
        }
    }

    @Override
    public long estimateSize(@Nullable List<Object> value) {
        assert value != null && value.size() == subEstimators.size()
            : "value must have the same number of items as there are keyTypes/sizeEstimators";

        long size = 0;
        for (int i = 0; i < value.size(); i++) {
            size += subEstimators.get(i).estimateSize(value.get(i));
        }
        return size;
    }
}
