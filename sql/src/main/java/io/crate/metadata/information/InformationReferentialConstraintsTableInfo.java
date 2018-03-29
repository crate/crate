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
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowContextCollectorExpression;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.types.DataTypes;

public class InformationReferentialConstraintsTableInfo extends InformationTableInfo {

    public static final String NAME = "referential_constraints";
    public static final RelationName IDENT = new RelationName(InformationSchemaInfo.NAME, NAME);

    public static class Columns {
        static final ColumnIdent CONSTRAINT_CATALOG = new ColumnIdent("constraint_catalog");
        static final ColumnIdent CONSTRAINT_SCHEMA = new ColumnIdent("constraint_schema");
        static final ColumnIdent CONSTRAINT_NAME = new ColumnIdent("constraint_name");
        static final ColumnIdent UNIQUE_CONSTRAINT_CATALOG = new ColumnIdent("unique_constraint_catalog");
        static final ColumnIdent UNIQUE_CONSTRAINT_SCHEMA = new ColumnIdent("unique_constraint_schema");
        static final ColumnIdent UNIQUE_CONSTRAINT_NAME = new ColumnIdent("unique_constraint_name");
        static final ColumnIdent MATCH_OPTION = new ColumnIdent("match_option");
        static final ColumnIdent UPDATE_RULE = new ColumnIdent("update_rule");
        static final ColumnIdent DELETE_RULE = new ColumnIdent("delete_rule");
    }

    private static ColumnRegistrar columnRegistrar() {
        return new ColumnRegistrar(IDENT, RowGranularity.DOC)
            .register(Columns.CONSTRAINT_CATALOG, DataTypes.STRING)
            .register(Columns.CONSTRAINT_SCHEMA, DataTypes.STRING)
            .register(Columns.CONSTRAINT_NAME, DataTypes.STRING)
            .register(Columns.UNIQUE_CONSTRAINT_CATALOG, DataTypes.STRING)
            .register(Columns.UNIQUE_CONSTRAINT_SCHEMA, DataTypes.STRING)
            .register(Columns.UNIQUE_CONSTRAINT_NAME, DataTypes.STRING)
            .register(Columns.MATCH_OPTION, DataTypes.STRING)
            .register(Columns.UPDATE_RULE, DataTypes.STRING)
            .register(Columns.DELETE_RULE, DataTypes.STRING);

    }

    public static ImmutableMap<ColumnIdent, RowCollectExpressionFactory<Void>> expressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<Void>>builder()
            .put(Columns.CONSTRAINT_CATALOG,
                () -> RowContextCollectorExpression.objToBytesRef(r -> null))
            .put(Columns.CONSTRAINT_SCHEMA,
                () -> RowContextCollectorExpression.objToBytesRef(r -> null))
            .put(Columns.CONSTRAINT_NAME,
                () -> RowContextCollectorExpression.objToBytesRef(r -> null))
            .put(Columns.UNIQUE_CONSTRAINT_CATALOG,
                () -> RowContextCollectorExpression.objToBytesRef(r -> null))
            .put(Columns.UNIQUE_CONSTRAINT_SCHEMA,
                () -> RowContextCollectorExpression.objToBytesRef(r -> null))
            .put(Columns.UNIQUE_CONSTRAINT_NAME,
                () -> RowContextCollectorExpression.objToBytesRef(r -> null))
            .put(Columns.MATCH_OPTION,
                () -> RowContextCollectorExpression.objToBytesRef(r -> null))
            .put(Columns.UPDATE_RULE,
                () -> RowContextCollectorExpression.objToBytesRef(r -> null))
            .put(Columns.DELETE_RULE,
                () -> RowContextCollectorExpression.objToBytesRef(r -> null))
            .build();
    }

    InformationReferentialConstraintsTableInfo() {
        super(
            IDENT,
            columnRegistrar(),
            ImmutableList.of(Columns.CONSTRAINT_CATALOG, Columns.CONSTRAINT_SCHEMA, Columns.CONSTRAINT_NAME)
        );
    }
}
