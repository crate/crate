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

package io.crate.metadata.information;

import io.crate.expression.reference.information.ColumnContext;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.RelationName;
import io.crate.metadata.SystemTable;
import io.crate.types.ByteType;
import io.crate.types.DataTypes;
import io.crate.types.DoubleType;
import io.crate.types.FloatType;
import io.crate.types.IntegerType;
import io.crate.types.LongType;
import io.crate.types.ShortType;
import io.crate.types.StringType;

import java.util.Map;

import static io.crate.types.DataTypes.BOOLEAN;
import static io.crate.types.DataTypes.INTEGER;
import static io.crate.types.DataTypes.STRING;
import static io.crate.types.DataTypes.TIMESTAMP;
import static io.crate.types.DataTypes.TIMESTAMPZ;


public class InformationColumnsTableInfo {

    public static final String NAME = "columns";
    public static final RelationName IDENT = new RelationName(InformationSchemaInfo.NAME, NAME);

    private static final String IS_GENERATED_NEVER = "NEVER";
    private static final String IS_GENERATED_ALWAYS = "ALWAYS";

    public static SystemTable<ColumnContext> create() {
        return SystemTable.<ColumnContext>builder(IDENT)
            .addNonNull("table_schema", STRING, r -> r.info.ident().tableIdent().schema())
            .addNonNull("table_name", STRING, r -> r.info.ident().tableIdent().name())
            .addNonNull("table_catalog", STRING, r -> r.info.ident().tableIdent().schema())
            .addNonNull("column_name", STRING, r -> r.info.column().sqlFqn())
            .addNonNull("ordinal_position", INTEGER, r -> r.info.position())
            .addNonNull("data_type", STRING, r -> r.info.valueType().getName())
            .addNonNull("is_generated", STRING, r -> {
                if (r.info instanceof GeneratedReference) {
                    return IS_GENERATED_ALWAYS;
                }
                return IS_GENERATED_NEVER;
            })
            .addNonNull("is_nullable", BOOLEAN, r -> !r.tableInfo.primaryKey().contains(r.info.column()) && r.info.isNullable())
            .add("generation_expression", STRING, r -> {
                if (r.info instanceof GeneratedReference) {
                    return ((GeneratedReference) r.info).formattedGeneratedExpression();
                }
                return null;
            })
            .add("column_default", STRING, r -> {
                Symbol defaultExpression = r.info.defaultExpression();
                if (defaultExpression != null) {
                    return defaultExpression.toString();
                } else {
                    return null;
                }
            })
            .add("character_maximum_length", INTEGER, r -> {
                if (r.info.valueType() instanceof StringType) {
                    var stringType = ((StringType) r.info.valueType());
                    if (stringType.unbound()) {
                        return null;
                    } else {
                        return stringType.lengthLimit();
                    }
                } else {
                    return null;
                }
            })
            .add("character_octet_length", INTEGER, ignored -> null)
            .add("numeric_precision", INTEGER, r -> PRECISION_BY_TYPE_ID.get(r.info.valueType().id()))
            .add("numeric_precision_radix", INTEGER, r -> {
                if (DataTypes.isNumericPrimitive(r.info.valueType())) {
                    return NUMERIC_PRECISION_RADIX;
                }
                return null;
            })
            .add("numeric_scale", INTEGER, ignored -> null)
            .add("datetime_precision", INTEGER, r -> {
                if (r.info.valueType() == TIMESTAMPZ || r.info.valueType() == TIMESTAMP) {
                    return DATETIME_PRECISION;
                }
                return null;
            })
            .add("interval_type", STRING, ignored -> null)
            .add("interval_precision", INTEGER, ignored -> null)
            .add("character_set_catalog", STRING, ignored -> null)
            .add("character_set_schema", STRING, ignored -> null)
            .add("character_set_name", STRING, ignored -> null)
            .add("collation_catalog", STRING, ignored -> null)
            .add("collation_schema", STRING, ignored -> null)
            .add("collation_name", STRING, ignored -> null)
            .add("domain_catalog", STRING, ignored -> null)
            .add("domain_schema", STRING, ignored -> null)
            .add("domain_name", STRING, ignored -> null)
            .add("udt_catalog", STRING, ignored -> null)
            .add("udt_schema", STRING, ignored -> null)
            .add("udt_name", STRING, ignored -> null)
            .add("check_references", STRING, ignored -> null)
            .add("check_action", INTEGER, ignored -> null)
            .setPrimaryKeys(
                new ColumnIdent("table_catalog"),
                new ColumnIdent("table_name"),
                new ColumnIdent("table_schema"),
                new ColumnIdent("column_name")
            )
            .build();
    }

    private static final Integer NUMERIC_PRECISION_RADIX = 2; // Binary
    private static final Integer DATETIME_PRECISION = 3; // Milliseconds

    /**
     * For floating point numbers please refer to:
     * https://en.wikipedia.org/wiki/IEEE_floating_point
     */
    private static final Map<Integer, Integer> PRECISION_BY_TYPE_ID = Map.ofEntries(
        Map.entry(ByteType.ID, 8),
        Map.entry(ShortType.ID, 16),
        Map.entry(FloatType.ID, 24),
        Map.entry(IntegerType.ID, 32),
        Map.entry(DoubleType.ID, 53),
        Map.entry(LongType.ID, 64)
    );
}
