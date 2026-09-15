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

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqList;
import io.crate.metadata.Reference;
import io.crate.parquet.exceptions.IncompatibleSchemaForParquetException;
import io.crate.types.ArrayType;
import io.crate.types.BitStringType;
import io.crate.types.BooleanType;
import io.crate.types.DataType;
import io.crate.types.DateType;
import io.crate.types.DoubleType;
import io.crate.types.FloatType;
import io.crate.types.IntegerType;
import io.crate.types.IpType;
import io.crate.types.LongType;
import io.crate.types.NumericType;
import io.crate.types.ObjectType;
import io.crate.types.ShortType;
import io.crate.types.StringType;
import io.crate.types.TimestampType;
import io.crate.types.UUIDType;

public class ParquetTypes {

    public static Object getObject(RowReader rowReader, Reference ref) throws IncompatibleSchemaForParquetException {
        DataType<?> crateType = ref.valueType();
        String fqn = ref.column().fqn();

        try {
            Object object = switch (crateType.id()) {
                case BooleanType.ID -> rowReader.getBoolean(fqn);
                case IntegerType.ID -> rowReader.getInt(fqn);
                case ShortType.ID -> {
                    yield ShortType.INSTANCE.implicitCast(rowReader.getInt(fqn));
                }
                case LongType.ID -> rowReader.getLong(fqn);
                case NumericType.ID -> rowReader.getDecimal(fqn);
                case FloatType.ID -> rowReader.getFloat(fqn);
                case DoubleType.ID -> rowReader.getDouble(fqn);
                case StringType.ID -> rowReader.getString(fqn);
                case DateType.ID -> {
                    LocalDate date = rowReader.getDate(fqn);
                    yield date == null ? null : date.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
                }
                case TimestampType.ID_WITH_TZ -> {
                    Instant instant = rowReader.getTimestamp(fqn);
                    yield instant == null ? null : instant.toEpochMilli();
                }
                case TimestampType.ID_WITHOUT_TZ -> {
                    LocalDateTime localDateTime = rowReader.getLocalTimestamp(fqn);
                    yield localDateTime == null ? null
                            : localDateTime.atZone(ZoneOffset.UTC).toInstant().toEpochMilli();
                }
                case UUIDType.ID -> rowReader.getUuid(fqn);
                case ObjectType.ID -> rowReader.getString(fqn);
                case ArrayType.ID -> {
                    PqList pqList = rowReader.getList(fqn);
                    yield pqList == null ? null : pqList.values();
                }
                case BitStringType.ID -> rowReader.getString(fqn);
                case IpType.ID -> {
                    try {
                        yield rowReader.getString(fqn);
                    } catch (RuntimeException e) {
                        yield rowReader.getLong(fqn);
                    }
                }
                default ->
                    throw new UnsupportedOperationException("The CrateDB type " + crateType.toString()
                            + " is not supported in the parquet foreign data wrapper");
            };
            return object;
        } catch (RuntimeException e) {
            throw new IncompatibleSchemaForParquetException(fqn, crateType);
        }
    }
}
