/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.metadata.information;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.execution.engine.collect.sources.InformationSchemaIterables;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.types.DataTypes;

import static io.crate.metadata.RowContextCollectorExpression.forFunction;
import static io.crate.metadata.RowContextCollectorExpression.objToBytesRef;
import static io.crate.execution.engine.collect.sources.InformationSchemaIterables.PK_SUFFIX;

/**
 * Table which contains the primary keys of all user tables.
 */
public class InformationKeyColumnUsageTableInfo extends InformationTableInfo {

    public static final String NAME = "key_column_usage";
    public static final RelationName IDENT = new RelationName(InformationSchemaInfo.NAME, NAME);

    public static class Columns {
        static final ColumnIdent CONSTRAINT_CATALOG = new ColumnIdent("constraint_catalog");
        static final ColumnIdent CONSTRAINT_SCHEMA = new ColumnIdent("constraint_schema");
        static final ColumnIdent CONSTRAINT_NAME = new ColumnIdent("constraint_name");
        static final ColumnIdent TABLE_CATALOG = new ColumnIdent("table_catalog");
        static final ColumnIdent TABLE_SCHEMA = new ColumnIdent("table_schema");
        static final ColumnIdent TABLE_NAME = new ColumnIdent("table_name");
        static final ColumnIdent COLUMN_NAME = new ColumnIdent("column_name");
        static final ColumnIdent ORDINAL_POSITION = new ColumnIdent("ordinal_position");
    }

    private static ColumnRegistrar columnRegistrar() {
        return new ColumnRegistrar(IDENT, RowGranularity.DOC)
            .register(Columns.CONSTRAINT_CATALOG, DataTypes.STRING)
            .register(Columns.CONSTRAINT_SCHEMA, DataTypes.STRING)
            .register(Columns.CONSTRAINT_NAME, DataTypes.STRING)
            .register(Columns.TABLE_CATALOG, DataTypes.STRING)
            .register(Columns.TABLE_SCHEMA, DataTypes.STRING)
            .register(Columns.TABLE_NAME, DataTypes.STRING)
            .register(Columns.COLUMN_NAME, DataTypes.STRING)
            .register(Columns.ORDINAL_POSITION, DataTypes.INTEGER);
    }

    public static ImmutableMap<ColumnIdent, RowCollectExpressionFactory<InformationSchemaIterables.KeyColumnUsage>> expressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<InformationSchemaIterables.KeyColumnUsage>>builder()
            .put(Columns.CONSTRAINT_CATALOG,
                () -> objToBytesRef(InformationSchemaIterables.KeyColumnUsage::getSchema))
            .put(Columns.CONSTRAINT_SCHEMA,
                () -> objToBytesRef(InformationSchemaIterables.KeyColumnUsage::getSchema))
            .put(Columns.CONSTRAINT_NAME,
                () -> objToBytesRef(k -> k.getTableName() + PK_SUFFIX))
            .put(Columns.TABLE_CATALOG,
                () -> objToBytesRef(InformationSchemaIterables.KeyColumnUsage::getSchema))
            .put(Columns.TABLE_SCHEMA,
                () -> objToBytesRef(InformationSchemaIterables.KeyColumnUsage::getSchema))
            .put(Columns.TABLE_NAME,
                () -> objToBytesRef(InformationSchemaIterables.KeyColumnUsage::getTableName))
            .put(Columns.COLUMN_NAME,
                () -> objToBytesRef(InformationSchemaIterables.KeyColumnUsage::getPkColumnName))
            .put(Columns.ORDINAL_POSITION,
                () -> forFunction(InformationSchemaIterables.KeyColumnUsage::getOrdinal))
            .build();
    }

    InformationKeyColumnUsageTableInfo() {
        super(
            IDENT,
            columnRegistrar(),
            ImmutableList.of()
        );
    }
}
