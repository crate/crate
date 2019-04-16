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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.execution.engine.collect.NestableCollectExpression;
import io.crate.expression.reference.information.ColumnContext;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.types.ByteType;
import io.crate.types.DataTypes;
import io.crate.types.DoubleType;
import io.crate.types.FloatType;
import io.crate.types.IntegerType;
import io.crate.types.LongType;
import io.crate.types.ShortType;
import org.elasticsearch.common.collect.MapBuilder;

import java.util.Map;

public class InformationColumnsTableInfo extends InformationTableInfo {

    public static final String NAME = "columns";
    public static final RelationName IDENT = new RelationName(InformationSchemaInfo.NAME, NAME);

    private static final String IS_GENERATED_NEVER = "NEVER";
    private static final String IS_GENERATED_ALWAYS = "ALWAYS";

    public static class Columns {
        static final ColumnIdent TABLE_SCHEMA = new ColumnIdent("table_schema");
        static final ColumnIdent TABLE_NAME = new ColumnIdent("table_name");
        static final ColumnIdent TABLE_CATALOG = new ColumnIdent("table_catalog");
        static final ColumnIdent COLUMN_NAME = new ColumnIdent("column_name");
        static final ColumnIdent ORDINAL_POSITION = new ColumnIdent("ordinal_position");
        static final ColumnIdent DATA_TYPE = new ColumnIdent("data_type");
        static final ColumnIdent IS_GENERATED = new ColumnIdent("is_generated");
        static final ColumnIdent IS_NULLABLE = new ColumnIdent("is_nullable");
        static final ColumnIdent GENERATION_EXPRESSION = new ColumnIdent("generation_expression");
        static final ColumnIdent COLUMN_DEFAULT = new ColumnIdent("column_default");
        static final ColumnIdent CHARACTER_MAXIMUM_LENGTH = new ColumnIdent("character_maximum_length");
        static final ColumnIdent CHARACTER_OCTET_LENGTH = new ColumnIdent("character_octet_length");
        static final ColumnIdent NUMERIC_PRECISION = new ColumnIdent("numeric_precision");
        static final ColumnIdent NUMERIC_PRECISION_RADIX = new ColumnIdent("numeric_precision_radix");
        static final ColumnIdent NUMERIC_SCALE = new ColumnIdent("numeric_scale");
        static final ColumnIdent DATETIME_PRECISION = new ColumnIdent("datetime_precision");
        static final ColumnIdent INTERVAL_TYPE = new ColumnIdent("interval_type");
        static final ColumnIdent INTERVAL_PRECISION = new ColumnIdent("interval_precision");
        static final ColumnIdent CHARACTER_SET_CATALOG = new ColumnIdent("character_set_catalog");
        static final ColumnIdent CHARACTER_SET_SCHEMA = new ColumnIdent("character_set_schema");
        static final ColumnIdent CHARACTER_SET_NAME = new ColumnIdent("character_set_name");
        static final ColumnIdent COLLATION_CATALOG = new ColumnIdent("collation_catalog");
        static final ColumnIdent COLLATION_SCHEMA = new ColumnIdent("collation_schema");
        static final ColumnIdent COLLATION_NAME = new ColumnIdent("collation_name");
        static final ColumnIdent DOMAIN_CATALOG = new ColumnIdent("domain_catalog");
        static final ColumnIdent DOMAIN_SCHEMA = new ColumnIdent("domain_schema");
        static final ColumnIdent DOMAIN_NAME = new ColumnIdent("domain_name");
        static final ColumnIdent USER_DEFINED_TYPE_CATALOG = new ColumnIdent("udt_catalog");
        static final ColumnIdent USER_DEFINED_TYPE_SCHEMA = new ColumnIdent("udt_schema");
        static final ColumnIdent USER_DEFINED_TYPE_NAME = new ColumnIdent("udt_name");
        static final ColumnIdent CHECK_REFERENCES = new ColumnIdent("check_references");
        static final ColumnIdent CHECK_ACTION = new ColumnIdent("check_action");
    }

    private static ColumnRegistrar columnRegistrar() {
        return new ColumnRegistrar(IDENT, RowGranularity.DOC)
            .register(Columns.TABLE_SCHEMA, DataTypes.STRING, false)
            .register(Columns.TABLE_NAME, DataTypes.STRING, false)
            .register(Columns.TABLE_CATALOG, DataTypes.STRING, false)
            .register(Columns.COLUMN_NAME, DataTypes.STRING, false)
            .register(Columns.ORDINAL_POSITION, DataTypes.INTEGER, false)
            .register(Columns.DATA_TYPE, DataTypes.STRING, false)
            .register(Columns.IS_GENERATED, DataTypes.STRING, false)
            .register(Columns.IS_NULLABLE, DataTypes.BOOLEAN, false)
            .register(Columns.GENERATION_EXPRESSION, DataTypes.STRING)
            .register(Columns.COLUMN_DEFAULT, DataTypes.STRING)
            .register(Columns.CHARACTER_MAXIMUM_LENGTH, DataTypes.INTEGER)
            .register(Columns.CHARACTER_OCTET_LENGTH, DataTypes.INTEGER)
            .register(Columns.NUMERIC_PRECISION, DataTypes.INTEGER)
            .register(Columns.NUMERIC_PRECISION_RADIX, DataTypes.INTEGER)
            .register(Columns.NUMERIC_SCALE, DataTypes.INTEGER)
            .register(Columns.DATETIME_PRECISION, DataTypes.INTEGER)
            .register(Columns.INTERVAL_TYPE, DataTypes.STRING)
            .register(Columns.INTERVAL_PRECISION, DataTypes.INTEGER)
            .register(Columns.CHARACTER_SET_CATALOG, DataTypes.STRING)
            .register(Columns.CHARACTER_SET_SCHEMA, DataTypes.STRING)
            .register(Columns.CHARACTER_SET_NAME, DataTypes.STRING)
            .register(Columns.COLLATION_CATALOG, DataTypes.STRING)
            .register(Columns.COLLATION_SCHEMA, DataTypes.STRING)
            .register(Columns.COLLATION_NAME, DataTypes.STRING)
            .register(Columns.DOMAIN_CATALOG, DataTypes.STRING)
            .register(Columns.DOMAIN_SCHEMA, DataTypes.STRING)
            .register(Columns.DOMAIN_NAME, DataTypes.STRING)
            .register(Columns.USER_DEFINED_TYPE_CATALOG, DataTypes.STRING)
            .register(Columns.USER_DEFINED_TYPE_SCHEMA, DataTypes.STRING)
            .register(Columns.USER_DEFINED_TYPE_NAME, DataTypes.STRING)
            .register(Columns.CHECK_REFERENCES, DataTypes.STRING)
            .register(Columns.CHECK_ACTION, DataTypes.INTEGER);
    }

    public static Map<ColumnIdent, RowCollectExpressionFactory<ColumnContext>> expression() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<ColumnContext>>builder()
            .put(Columns.TABLE_SCHEMA,
                () -> NestableCollectExpression.forFunction(r -> r.info.ident().tableIdent().schema()))
            .put(Columns.TABLE_NAME,
                () -> NestableCollectExpression.forFunction(r -> r.info.ident().tableIdent().name()))
            .put(Columns.TABLE_CATALOG,
                () -> NestableCollectExpression.forFunction(r -> r.info.ident().tableIdent().schema()))
            .put(Columns.COLUMN_NAME,
                () -> NestableCollectExpression.forFunction(r -> r.info.column().sqlFqn()))
            .put(Columns.ORDINAL_POSITION,
                () -> NestableCollectExpression.forFunction(ColumnContext::getOrdinal))
            .put(Columns.DATA_TYPE,
                () -> NestableCollectExpression.forFunction(r -> r.info.valueType().getName()))
            .put(Columns.COLUMN_DEFAULT,
                () -> NestableCollectExpression.constant(null))
            .put(Columns.CHARACTER_MAXIMUM_LENGTH,
                () -> NestableCollectExpression.constant(null))
            .put(Columns.CHARACTER_OCTET_LENGTH,
                () -> NestableCollectExpression.constant(null))
            .put(Columns.NUMERIC_PRECISION,
                () -> NestableCollectExpression.forFunction(r -> PRECISION_BY_TYPE_ID.get(r.info.valueType().id())))
            .put(Columns.NUMERIC_PRECISION_RADIX,
                () -> NestableCollectExpression.forFunction(r -> {
                    if (DataTypes.NUMERIC_PRIMITIVE_TYPES.contains(r.info.valueType())) {
                        return NUMERIC_PRECISION_RADIX;
                    }
                    return null;
                }))
            .put(Columns.NUMERIC_SCALE,
                () -> NestableCollectExpression.constant(null))
            .put(Columns.DATETIME_PRECISION,
                () -> NestableCollectExpression.forFunction(r -> {
                    if (r.info.valueType() == DataTypes.TIMESTAMPZ
                        || r.info.valueType() == DataTypes.TIMESTAMP) {
                        return DATETIME_PRECISION;
                    }
                    return null;
                }))
            .put(Columns.INTERVAL_TYPE,
                () -> NestableCollectExpression.constant(null))
            .put(Columns.INTERVAL_PRECISION,
                () -> NestableCollectExpression.constant(null))
            .put(Columns.CHARACTER_SET_CATALOG,
                () -> NestableCollectExpression.constant(null))
            .put(Columns.CHARACTER_SET_SCHEMA,
                () -> NestableCollectExpression.constant(null))
            .put(Columns.CHARACTER_SET_NAME,
                () -> NestableCollectExpression.constant(null))
            .put(Columns.COLLATION_CATALOG,
                () -> NestableCollectExpression.constant(null))
            .put(Columns.COLLATION_SCHEMA,
                () -> NestableCollectExpression.constant(null))
            .put(Columns.COLLATION_NAME,
                () -> NestableCollectExpression.constant(null))
            .put(Columns.DOMAIN_CATALOG,
                () -> NestableCollectExpression.constant(null))
            .put(Columns.DOMAIN_SCHEMA,
                () -> NestableCollectExpression.constant(null))
            .put(Columns.DOMAIN_NAME,
                () -> NestableCollectExpression.constant(null))
            .put(Columns.USER_DEFINED_TYPE_CATALOG,
                () -> NestableCollectExpression.constant(null))
            .put(Columns.USER_DEFINED_TYPE_SCHEMA,
                () -> NestableCollectExpression.constant(null))
            .put(Columns.USER_DEFINED_TYPE_NAME,
                () -> NestableCollectExpression.constant(null))
            .put(Columns.CHECK_REFERENCES,
                () -> NestableCollectExpression.constant(null))
            .put(Columns.CHECK_ACTION,
                () -> NestableCollectExpression.constant(null))
            .put(Columns.IS_GENERATED,
                () -> NestableCollectExpression.forFunction(r -> {
                    if (r.info instanceof GeneratedReference) {
                        return IS_GENERATED_ALWAYS;
                    }
                    return IS_GENERATED_NEVER;
                }))
            .put(Columns.IS_NULLABLE,
                () -> NestableCollectExpression.forFunction(r ->
                    !r.tableInfo.primaryKey().contains(r.info.column()) && r.info.isNullable()))
            .put(Columns.GENERATION_EXPRESSION,
                () -> NestableCollectExpression.forFunction(r -> {
                    if (r.info instanceof GeneratedReference) {
                        return ((GeneratedReference) r.info).formattedGeneratedExpression();
                    }
                    return null;
                }))
            .build();
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
        super(
            IDENT,
            columnRegistrar(),
            ImmutableList.of(Columns.TABLE_CATALOG, Columns.TABLE_NAME, Columns.TABLE_SCHEMA, Columns.COLUMN_NAME)
        );
    }
}
