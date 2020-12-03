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

package io.crate.breaker;

import io.crate.common.collections.Lists2;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.FixedWidthType;
import io.crate.types.GeoShapeType;
import io.crate.types.IpType;
import io.crate.types.NumericType;
import io.crate.types.ObjectType;
import io.crate.types.OidVectorType;
import io.crate.types.RegprocType;
import io.crate.types.RowType;
import io.crate.types.StringType;
import io.crate.types.UndefinedType;

import java.util.Locale;

import static org.apache.lucene.util.RamUsageEstimator.UNKNOWN_DEFAULT_RAM_BYTES_USED;

public class SizeEstimatorFactory {

    private static final int SAMPLE_EVERY_NTH = 100;

    @SuppressWarnings("unchecked")
    public static <T> SizeEstimator<T> create(DataType<?> type) {
        switch (type.id()) {
            case UndefinedType.ID:
                return (SizeEstimator<T>) new ConstSizeEstimator(UNKNOWN_DEFAULT_RAM_BYTES_USED);

            case ObjectType.ID:
            case GeoShapeType.ID:
                return (SizeEstimator<T>) new SamplingSizeEstimator<>(SAMPLE_EVERY_NTH, MapSizeEstimator.INSTANCE);

            case StringType.ID:
            case IpType.ID:
                return (SizeEstimator<T>) StringSizeEstimator.INSTANCE;

            case ArrayType.ID:
                var innerEstimator = create(((ArrayType<?>) type).innerType());
                return (SizeEstimator<T>) ArraySizeEstimator.create(innerEstimator);

            case OidVectorType.ID:
                return (SizeEstimator<T>) ArraySizeEstimator.create(create(DataTypes.INTEGER));

            case RowType.ID:
                return (SizeEstimator<T>) new RecordSizeEstimator(Lists2.map(((RowType) type).fieldTypes(), SizeEstimatorFactory::create));

            case RegprocType.ID:
                return (SizeEstimator<T>) RegprocSizeEstimator.INSTANCE;

            case NumericType.ID:
                return (SizeEstimator<T>) NumericSizeEstimator.INSTANCE;

            default:
                if (type instanceof FixedWidthType) {
                    return (SizeEstimator<T>) new ConstSizeEstimator(((FixedWidthType) type).fixedSize());
                }
                throw new UnsupportedOperationException(String.format(Locale.ENGLISH, "Cannot get SizeEstimator for type %s", type));
        }
    }
}
