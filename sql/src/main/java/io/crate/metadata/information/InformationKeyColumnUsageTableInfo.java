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

import io.crate.execution.engine.collect.sources.InformationSchemaIterables;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.table.ColumnRegistrar;

import java.util.Map;

import static io.crate.execution.engine.collect.NestableCollectExpression.forFunction;
import static io.crate.execution.engine.collect.sources.InformationSchemaIterables.PK_SUFFIX;
import static io.crate.types.DataTypes.STRING;
import static io.crate.types.DataTypes.INTEGER;

/**
 * Table which contains the primary keys of all user tables.
 */
public class InformationKeyColumnUsageTableInfo extends InformationTableInfo<InformationSchemaIterables.KeyColumnUsage> {

    public static final String NAME = "key_column_usage";
    public static final RelationName IDENT = new RelationName(InformationSchemaInfo.NAME, NAME);

    private static ColumnRegistrar<InformationSchemaIterables.KeyColumnUsage> columnRegistrar() {
        return new ColumnRegistrar<InformationSchemaIterables.KeyColumnUsage>(IDENT, RowGranularity.DOC)
            .register("constraint_catalog", STRING, () -> forFunction(InformationSchemaIterables.KeyColumnUsage::getSchema))
            .register("constraint_schema", STRING, () -> forFunction(InformationSchemaIterables.KeyColumnUsage::getSchema))
            .register("constraint_name", STRING, () -> forFunction(k -> k.getTableName() + PK_SUFFIX))
            .register("table_catalog", STRING, () -> forFunction(InformationSchemaIterables.KeyColumnUsage::getSchema))
            .register("table_schema", STRING, () -> forFunction(InformationSchemaIterables.KeyColumnUsage::getSchema))
            .register("table_name", STRING, () -> forFunction(InformationSchemaIterables.KeyColumnUsage::getTableName))
            .register("column_name", STRING, () -> forFunction(InformationSchemaIterables.KeyColumnUsage::getPkColumnIdent))
            .register("ordinal_position", INTEGER, () -> forFunction(InformationSchemaIterables.KeyColumnUsage::getOrdinal));
    }

    static Map<ColumnIdent, RowCollectExpressionFactory<InformationSchemaIterables.KeyColumnUsage>> expressions() {
        return columnRegistrar().expressions();
    }

    InformationKeyColumnUsageTableInfo() {
        super(IDENT, columnRegistrar(), "constraint_catalog", "constraint_schema", "constraint_name", "column_name");
    }
}
