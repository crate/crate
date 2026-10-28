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

package io.crate.parquet.exceptions;

import java.util.Locale;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.schema.SchemaNode;
import dev.hardwood.schema.SchemaNode.GroupNode;
import dev.hardwood.schema.SchemaNode.PrimitiveNode;
import io.crate.metadata.ColumnIdent;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.DataType;

public class IncompatibleSchemaForParquet extends RuntimeException {
    private IncompatibleSchemaForParquet(String message) {
        super(message);
    }

    public IncompatibleSchemaForParquet(ColumnIdent columnIdent, DataType<?> dataType,
            SchemaNode schemaNode) {
        super(message(columnIdent, dataType, schemaNode));
    }

    private static String message(ColumnIdent col, DataType<?> dataType, SchemaNode schemaNode) {
        String actualParquetType = determineActualParquetType(schemaNode);
        return String.format(
                Locale.ENGLISH,
                "Column `%s` has type `%s` in the foreign table, but in the parquet file it has type `%s`. These are incompatible.",
                col,
                dataType.getName(),
                actualParquetType);
    }

    public static IncompatibleSchemaForParquet forObjectType(ColumnIdent col, ColumnPolicy policy, SchemaNode schemaNode) {
        String actualParquetType = determineActualParquetType(schemaNode);
        String expectedParquetType = policy == ColumnPolicy.STRICT ? "STRUCT" : "JSON";
        String msg = String.format(
                Locale.ENGLISH,
                """
                        Column `%s` has type: `object` with column policy: `%s` in the foreign table.
                        This type and column policy requires the column in the Parquet file to be stored as `%s`.
                        The column in the Parquet file has type `%s`.
                        """,
                col,
                policy,
                expectedParquetType,
                actualParquetType);
        return new IncompatibleSchemaForParquet(msg);

    }

    public static String determineActualParquetType(SchemaNode schemaNode) {
        if (schemaNode instanceof PrimitiveNode node) {
            LogicalType logicalType = node.logicalType();
            return logicalType != null ? logicalType.toString() : node.type().toString();
        }

        if (schemaNode instanceof GroupNode node) {
            if (node.isMap()) {
                return "MAP";
            } else if (node.isVariant()) {
                return "VARIANT";
            } else if (node.isList()) {
                return "LIST";
            } else if (node.isStruct()) {
                return "STRUCT";
            }
        }
        return "UNKNOWN";
    }


}
