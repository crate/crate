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

import java.util.HashMap;
import java.util.Map;

import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqStruct;
import dev.hardwood.row.StructAccessor;
import dev.hardwood.schema.FileSchema;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.parquet.exceptions.IncompatibleSchemaForParquet;
import io.crate.sql.tree.ColumnPolicy;
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
    private static Object readValue(StructAccessor accessor, DataType<?> crateType, String fieldName, FileSchema fileSchema, ColumnIdent col) throws IncompatibleSchemaForParquet {
        try {
            if (accessor.isNull(fieldName)) {
                return null;
            }
            Object object = switch (crateType.id()) {
                case BooleanType.ID -> accessor.getBoolean(fieldName);
                case IntegerType.ID -> accessor.getInt(fieldName);
                case ShortType.ID -> crateType.implicitCast(accessor.getInt(fieldName));
                case LongType.ID -> accessor.getLong(fieldName);
                case NumericType.ID -> crateType.implicitCast(accessor.getDecimal(fieldName));
                case FloatType.ID -> accessor.getFloat(fieldName);
                case DoubleType.ID -> accessor.getDouble(fieldName);
                case StringType.ID -> accessor.getString(fieldName);
                case DateType.ID -> crateType.implicitCast(accessor.getDate(fieldName));
                case TimestampType.ID_WITH_TZ -> crateType.implicitCast(accessor.getTimestamp(fieldName));
                case TimestampType.ID_WITHOUT_TZ -> crateType.implicitCast(accessor.getLocalTimestamp(fieldName));
                case UUIDType.ID -> accessor.getUuid(fieldName);
                case ObjectType.ID -> readObjectType((ObjectType) crateType, accessor, fieldName, fileSchema, col);
                case ArrayType.ID -> crateType.implicitCast(accessor.getList(fieldName).values());
                case BitStringType.ID -> crateType.implicitCast(accessor.getString(fieldName));
                case IpType.ID -> {
                    try {
                        yield crateType.implicitCast(accessor.getString(fieldName));
                    } catch (ClassCastException e) {
                        yield crateType.implicitCast(accessor.getLong(fieldName));
                    }
                }
                default -> throw new UnsupportedOperationException("The CrateDB type " + crateType.toString()
                        + " is not supported in the parquet foreign data wrapper");
            };
            return object;
        } catch (ClassCastException e) {
            throw new IncompatibleSchemaForParquet(col, crateType, fileSchema.getColumn(fieldName));
        }
    }

    private static Map<String, Object> readObjectType(ObjectType objectType,
                                                      StructAccessor accessor,
                                                      String fqn,
                                                      FileSchema fileSchema,
                                                      ColumnIdent col) throws IncompatibleSchemaForParquet {
        ColumnPolicy policy = objectType.columnPolicy();
        try {
            return switch (policy) {
                case DYNAMIC, IGNORED -> objectType.implicitCast(accessor.getString(fqn));
                case STRICT -> {
                    PqStruct pqStruct = accessor.getStruct(fqn);
                    if (pqStruct == null) {
                        yield null;
                    }
                    Map<String, Object> result = new HashMap<>();
                    Map<String, DataType<?>> innerTypes = objectType.innerTypes();
                    for (Map.Entry<String, DataType<?>> innerType : innerTypes.entrySet()) {
                        String fieldName = innerType.getKey();
                        DataType<?> crateType = innerType.getValue();
                        if (crateType instanceof ObjectType innerObject) {
                            result.put(fieldName, readObjectType(innerObject, pqStruct, fieldName, fileSchema, col));
                        } else {
                            result.put(fieldName, readValue(pqStruct, crateType, fieldName, fileSchema, col));
                        }
                    }
                    yield result;

                }
                default -> throw new UnsupportedOperationException(
                        "Parquet foreign data wrapper does not support OBJECT column policy "
                                + objectType.columnPolicy());
            };
        } catch (ClassCastException e) {
            throw IncompatibleSchemaForParquet.forObjectType(col, policy, fileSchema);
        }
    }

    public static Object readColumn(FileSchema fileSchema, RowReader rowReader, Reference ref) {
        ColumnIdent col = ref.column();
        String fqn = col.fqn();
        return readValue(rowReader, ref.valueType(), fqn, fileSchema, col);

    }
}
