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

package io.crate.parquet;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import dev.hardwood.reader.RowReader;

import io.crate.metadata.Reference;
import io.crate.types.ArrayType;
import io.crate.types.BooleanType;
import io.crate.types.DataType;
import io.crate.types.DateType;
import io.crate.types.DoubleType;
import io.crate.types.FloatType;
import io.crate.types.IntegerType;
import io.crate.types.IntervalType;
import io.crate.types.LongType;
import io.crate.types.NumericType;
import io.crate.types.ObjectType;
import io.crate.types.StringType;
import io.crate.types.TimeTZType;
import io.crate.types.TimestampType;
import io.crate.types.UUIDType;

public class ParquetTypes {

    public static Object getObject(RowReader rowReader, Reference ref) {
        DataType<?> crateType = ref.valueType();
        String fqn = ref.column().fqn();

        Object object = switch (crateType.id()) {
            case BooleanType.ID -> rowReader.getBoolean(fqn);
            case IntegerType.ID -> rowReader.getInt(fqn);
            case LongType.ID -> rowReader.getLong(fqn);
            case FloatType.ID -> rowReader.getFloat(fqn);
            case DoubleType.ID -> rowReader.getDouble(fqn);
            case StringType.ID -> rowReader.getString(fqn);
            case DateType.ID -> rowReader.getDate(fqn);
            // TODO: timestamp types need to be hardened; look at this again later
            case TimeTZType.ID -> rowReader.getTimestamp(fqn);
            case TimestampType.ID_WITHOUT_TZ -> {
                LocalDateTime ldt = rowReader.getLocalTimestamp(fqn);
                yield ldt == null ? null : ldt.atZone(ZoneOffset.UTC).toInstant().toEpochMilli();
            }
            case NumericType.ID -> rowReader.getDecimal(fqn);
            case UUIDType.ID -> rowReader.getUuid(fqn);
            case IntervalType.ID -> rowReader.getInterval(fqn);
            case ObjectType.ID -> rowReader.getStruct(fqn);
            case ArrayType.ID -> rowReader.getList(fqn);
            default ->
                throw new UnsupportedOperationException("The CrateDB type " + crateType.toString()
                        + " is not supported in the parquet foreign data wrapper");
        };
        return object;
    }
}
