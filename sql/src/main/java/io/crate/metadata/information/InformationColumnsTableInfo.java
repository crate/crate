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

package io.crate.metadata.information;

import io.crate.expression.reference.information.ColumnContext;
import io.crate.expression.symbol.format.SymbolPrinter;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.types.ByteType;
import io.crate.types.DoubleType;
import io.crate.types.FloatType;
import io.crate.types.IntegerType;
import io.crate.types.LongType;
import io.crate.types.ShortType;
import org.elasticsearch.common.collect.MapBuilder;

import java.util.Map;
import static io.crate.types.DataTypes.STRING;
import static io.crate.types.DataTypes.INTEGER;
import static io.crate.types.DataTypes.BOOLEAN;
import static io.crate.types.DataTypes.TIMESTAMP;
import static io.crate.types.DataTypes.TIMESTAMPZ;
import static io.crate.types.DataTypes.NUMERIC_PRIMITIVE_TYPES;
import static io.crate.execution.engine.collect.NestableCollectExpression.forFunction;
import static io.crate.execution.engine.collect.NestableCollectExpression.constant;


public class InformationColumnsTableInfo extends InformationTableInfo<ColumnContext> {

    public static final String NAME = "columns";
    public static final RelationName IDENT = new RelationName(InformationSchemaInfo.NAME, NAME);

    private static final String IS_GENERATED_NEVER = "NEVER";
    private static final String IS_GENERATED_ALWAYS = "ALWAYS";

    private static ColumnRegistrar<ColumnContext> columnRegistrar() {
        return new ColumnRegistrar<ColumnContext>(IDENT, RowGranularity.DOC)
            .register("table_schema", STRING, false,
                () -> forFunction(r -> r.info.ident().tableIdent().schema()))
            .register("table_name", STRING, false,
                () -> forFunction(r -> r.info.ident().tableIdent().name()))
            .register("table_catalog", STRING, false,
                () -> forFunction(r -> r.info.ident().tableIdent().schema()))
            .register("column_name", STRING, false,
                () -> forFunction(r -> r.info.column().sqlFqn()))
            .register("ordinal_position", INTEGER, false,
                () -> forFunction(ColumnContext::getOrdinal))
            .register("data_type", STRING, false,
                () -> forFunction(r -> r.info.valueType().getName()))
            .register("is_generated", STRING, false,
                () -> forFunction(r -> {
                    if (r.info instanceof GeneratedReference) {
                        return IS_GENERATED_ALWAYS;
                    }
                    return IS_GENERATED_NEVER;
                }))
            .register("is_nullable", BOOLEAN, false,
                () -> forFunction(r -> !r.tableInfo.primaryKey().contains(r.info.column()) && r.info.isNullable()))

            .register("generation_expression", STRING,
                () -> forFunction(r -> {
                    if (r.info instanceof GeneratedReference) {
                        return ((GeneratedReference) r.info).formattedGeneratedExpression();
                    }
                    return null;
                }))
            .register("column_default", STRING,
                () -> forFunction(r -> {
                    if (r.info.defaultExpression() != null) {
                        return SymbolPrinter.INSTANCE.printUnqualified(r.info.defaultExpression());
                    } else {
                        return null;
                    }
                }))
            .register("character_maximum_length", INTEGER, () -> constant(null))
            .register("character_octet_length", INTEGER, () -> constant(null))
            .register("numeric_precision", INTEGER, () -> forFunction(r -> PRECISION_BY_TYPE_ID.get(r.info.valueType().id())))
            .register("numeric_precision_radix", INTEGER,
                () -> forFunction(r -> {
                    if (NUMERIC_PRIMITIVE_TYPES.contains(r.info.valueType())) {
                        return NUMERIC_PRECISION_RADIX;
                    }
                    return null;
                }))
            .register("numeric_scale", INTEGER, () -> constant(null))

            .register("datetime_precision", INTEGER, () -> forFunction(r -> {
                if (r.info.valueType() == TIMESTAMPZ
                    || r.info.valueType() == TIMESTAMP) {
                    return DATETIME_PRECISION;
                }
                return null;
            }))
            .register("interval_type", STRING, () -> constant(null))
            .register("interval_precision", INTEGER, () -> constant(null))
            .register("character_set_catalog", STRING, () -> constant(null))
            .register("character_set_schema", STRING, () -> constant(null))
            .register("character_set_name", STRING, () -> constant(null))
            .register("collation_catalog", STRING, () -> constant(null))
            .register("collation_schema", STRING, () -> constant(null))
            .register("collation_name", STRING, () -> constant(null))
            .register("domain_catalog", STRING, () -> constant(null))
            .register("domain_schema", STRING, () -> constant(null))
            .register("domain_name", STRING, () -> constant(null))
            .register("udt_catalog", STRING, () -> constant(null))
            .register("udt_schema", STRING, () -> constant(null))
            .register("udt_name", STRING, () -> constant(null))
            .register("check_references", STRING, () -> constant(null))
            .register("check_action", INTEGER, () -> constant(null));
    }

    static Map<ColumnIdent, RowCollectExpressionFactory<ColumnContext>> expression() {
        return columnRegistrar().expressions();
    }

    private static final Integer NUMERIC_PRECISION_RADIX = 2; // Binary
    private static final Integer DATETIME_PRECISION = 3; // Milliseconds

    /**
     * For floating point numbers please refer to:
     * https://en.wikipedia.org/wiki/IEEE_floating_point
     */
    private static final Map<Integer, Integer> PRECISION_BY_TYPE_ID = new MapBuilder<Integer, Integer>()
        .put(ByteType.ID, 8)
        .put(ShortType.ID, 16)
        .put(FloatType.ID, 24)
        .put(IntegerType.ID, 32)
        .put(DoubleType.ID, 53)
        .put(LongType.ID, 64)
        .map();

    InformationColumnsTableInfo() {
        super(IDENT, columnRegistrar(), "table_catalog", "table_name", "table_schema", "column_name");
    }
}
