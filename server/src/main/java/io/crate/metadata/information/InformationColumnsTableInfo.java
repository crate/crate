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

import static io.crate.types.DataTypes.BOOLEAN;
import static io.crate.types.DataTypes.INTEGER;
import static io.crate.types.DataTypes.STRING;
import static io.crate.types.DataTypes.STRING_ARRAY;
import static io.crate.types.DataTypes.TIMESTAMP;
import static io.crate.types.DataTypes.TIMESTAMPZ;

import io.crate.Constants;
import io.crate.expression.reference.information.ColumnContext;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.RelationName;
import io.crate.metadata.SystemTable;
import io.crate.types.DataTypes;


public class InformationColumnsTableInfo {

    public static final String NAME = "columns";
    public static final RelationName IDENT = new RelationName(InformationSchemaInfo.NAME, NAME);

    private static final String IS_GENERATED_NEVER = "NEVER";
    private static final String IS_GENERATED_ALWAYS = "ALWAYS";

    public static SystemTable<ColumnContext> create() {
        return SystemTable.<ColumnContext>builder(IDENT)
            .addNonNull("table_schema", STRING, r -> r.ref().ident().tableIdent().schema())
            .addNonNull("table_name", STRING, r -> r.ref().ident().tableIdent().name())
            .addNonNull("table_catalog", STRING, r -> Constants.DB_NAME)
            .addNonNull("column_name", STRING, r -> r.ref().column().sqlFqn())
            .addNonNull("ordinal_position", INTEGER, r -> r.ref().position())
            .addNonNull("data_type", STRING, r -> r.ref().valueType().getName())
            .addNonNull("is_generated", STRING, r -> {
                if (r.ref() instanceof GeneratedReference) {
                    return IS_GENERATED_ALWAYS;
                }
                return IS_GENERATED_NEVER;
            })
            .addNonNull("is_nullable", BOOLEAN, r -> !r.relation().primaryKey().contains(r.ref().column()) && r.ref().isNullable())
            .add("generation_expression", STRING, r -> {
                if (r.ref() instanceof GeneratedReference) {
                    return ((GeneratedReference) r.ref()).formattedGeneratedExpression();
                }
                return null;
            })
            .add("column_default", STRING, r -> {
                Symbol defaultExpression = r.ref().defaultExpression();
                if (defaultExpression != null) {
                    return defaultExpression.toString();
                } else {
                    return null;
                }
            })
            .add("character_maximum_length", INTEGER, r -> r.ref().valueType().characterMaximumLength())
            .add("character_octet_length", INTEGER, ignored -> null)
            .add("numeric_precision", INTEGER, r -> r.ref().valueType().numericPrecision())
            .add("numeric_precision_radix", INTEGER, r -> {
                if (DataTypes.isNumericPrimitive(r.ref().valueType())) {
                    return NUMERIC_PRECISION_RADIX;
                }
                return null;
            })
            .add("numeric_scale", INTEGER, ignored -> null)
            .add("datetime_precision", INTEGER, r -> {
                if (r.ref().valueType() == TIMESTAMPZ || r.ref().valueType() == TIMESTAMP) {
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
            .add("is_identity", BOOLEAN, ignored -> false)
            .add("identity_generation", STRING, ignored -> null)
            .add("identity_start", STRING, ignored -> null)
            .add("identity_increment", STRING, ignored -> null)
            .add("identity_maximum", STRING, ignored -> null)
            .add("identity_minimum", STRING, ignored -> null)
            .add("identity_cycle", BOOLEAN, ignored -> null)
            .add("check_references", STRING, ignored -> null)
            .add("check_action", INTEGER, ignored -> null)
            .startObject("column_details")
                .add("name", STRING , r -> r.ref().column().name())
                .add("path", STRING_ARRAY, r -> r.ref().column().path())
            .endObject()
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
}
