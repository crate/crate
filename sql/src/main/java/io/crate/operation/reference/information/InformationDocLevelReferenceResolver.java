/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operation.reference.information;

import com.google.common.collect.ImmutableMap;
import io.crate.metadata.*;
import io.crate.operation.reference.DocLevelReferenceResolver;
import io.crate.operation.reference.RowCollectNestedObjectExpression;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.util.Collection;
import java.util.Map;

@Singleton
public class InformationDocLevelReferenceResolver implements DocLevelReferenceResolver<RowCollectExpression<?, ?>> {

    private final ImmutableMap<ReferenceIdent, RowCollectExpression<?, ?>> implementations;

    @Inject
    public InformationDocLevelReferenceResolver() {
        ImmutableMap.Builder<ReferenceIdent, RowCollectExpression<?, ?>> builder = ImmutableMap.builder();

        // information_schema.tables
        builder.put(InformationTablesExpression.SCHEMA_NAME_EXPRESSION.info().ident(),
                InformationTablesExpression.SCHEMA_NAME_EXPRESSION);
        builder.put(InformationTablesExpression.TABLE_NAME_EXPRESSION.info().ident(),
                InformationTablesExpression.TABLE_NAME_EXPRESSION);
        builder.put(InformationTablesExpression.NUMBER_OF_SHARDS_EXPRESSION.info().ident(),
                InformationTablesExpression.NUMBER_OF_SHARDS_EXPRESSION);
        builder.put(InformationTablesExpression.NUMBER_OF_REPLICAS_EXPRESSION.info().ident(),
                InformationTablesExpression.NUMBER_OF_REPLICAS_EXPRESSION);
        builder.put(InformationTablesExpression.CLUSTERED_BY_EXPRESSION.info().ident(),
                InformationTablesExpression.CLUSTERED_BY_EXPRESSION);
        builder.put(InformationTablesExpression.PARTITION_BY_EXPRESSION.info().ident(),
                InformationTablesExpression.PARTITION_BY_EXPRESSION);
        builder.put(InformationTablesExpression.BLOB_PATH_EXPRESSION.info().ident(),
                InformationTablesExpression.BLOB_PATH_EXPRESSION);
        builder.put(InformationTablesExpression.SETTINGS_EXPRESSION.info().ident(),
                InformationTablesExpression.SETTINGS_EXPRESSION);
        addChildImplementationToBuilder(builder, InformationTablesExpression.SETTINGS_EXPRESSION);


        // information_schema.columns
        builder.put(InformationColumnsExpression.SCHEMA_NAME_EXPRESSION.info().ident(),
                InformationColumnsExpression.SCHEMA_NAME_EXPRESSION);
        builder.put(InformationColumnsExpression.TABLE_NAME_EXPRESSION.info().ident(),
                InformationColumnsExpression.TABLE_NAME_EXPRESSION);
        builder.put(InformationColumnsExpression.COLUMN_NAME_EXPRESSION.info().ident(),
                InformationColumnsExpression.COLUMN_NAME_EXPRESSION);
        builder.put(InformationColumnsExpression.ORDINAL_EXPRESSION.info().ident(),
                InformationColumnsExpression.ORDINAL_EXPRESSION);
        builder.put(InformationColumnsExpression.DATA_TYPE_EXPRESSION.info().ident(),
                InformationColumnsExpression.DATA_TYPE_EXPRESSION);

        // information_schema.table_partitions
        builder.put(InformationTablePartitionsExpression.TABLE_NAME_EXPRESSION.info().ident(),
                InformationTablePartitionsExpression.TABLE_NAME_EXPRESSION);
        builder.put(InformationTablePartitionsExpression.SCHEMA_NAME_EXPRESSION.info().ident(),
                InformationTablePartitionsExpression.SCHEMA_NAME_EXPRESSION);
        builder.put(InformationTablePartitionsExpression.PARTITION_IDENT_EXPRESSION.info().ident(),
                InformationTablePartitionsExpression.PARTITION_IDENT_EXPRESSION);
        builder.put(InformationTablePartitionsExpression.VALUES_EXPRESSION.info().ident(),
                InformationTablePartitionsExpression.VALUES_EXPRESSION);
        builder.put(InformationTablePartitionsExpression.NUMBER_OF_SHARDS_EXPRESSION.info().ident(),
                InformationTablePartitionsExpression.NUMBER_OF_SHARDS_EXPRESSION);
        builder.put(InformationTablePartitionsExpression.NUMBER_OF_REPLICAS_EXPRESSION.info().ident(),
                InformationTablePartitionsExpression.NUMBER_OF_REPLICAS_EXPRESSION);

        // information_schema.table_constraints
        builder.put(InformationTableConstraintsExpression.SCHEMA_NAME_EXPRESSION.info().ident(),
                InformationTableConstraintsExpression.SCHEMA_NAME_EXPRESSION);
        builder.put(InformationTableConstraintsExpression.TABLE_NAME_EXPRESSION.info().ident(),
                InformationTableConstraintsExpression.TABLE_NAME_EXPRESSION);
        builder.put(InformationTableConstraintsExpression.CONSTRAINT_NAME_EXPRESSION.info().ident(),
                InformationTableConstraintsExpression.CONSTRAINT_NAME_EXPRESSION);
        builder.put(InformationTableConstraintsExpression.CONSTRAINT_TYPE_EXPRESSION.info().ident(),
                InformationTableConstraintsExpression.CONSTRAINT_TYPE_EXPRESSION);

        // information_schema.routines
        builder.put(InformationRoutinesExpression.ROUTINE_NAME_EXPRESSION.info().ident(),
                InformationRoutinesExpression.ROUTINE_NAME_EXPRESSION);
        builder.put(InformationRoutinesExpression.ROUTINE_TYPE_EXPRESSION.info().ident(),
                InformationRoutinesExpression.ROUTINE_TYPE_EXPRESSION);

        // information_schema.schemata
        builder.put(InformationSchemataExpression.SCHEMA_NAME_EXPRESSION.info().ident(),
                InformationSchemataExpression.SCHEMA_NAME_EXPRESSION);

        implementations = builder.build();
    }


    private void addChildImplementationToBuilder(ImmutableMap.Builder<ReferenceIdent, RowCollectExpression<?, ?>> builder, RowCollectNestedObjectExpression parent) {
        for (Map.Entry<String, ReferenceImplementation> e : parent.getChildImplementations().entrySet()) {
            if (e.getValue() instanceof RowCollectNestedObjectExpression) {
                addChildImplementationToBuilder(builder, (RowCollectNestedObjectExpression) e.getValue());
            }
            if (e.getValue() instanceof RowCollectExpression) {
                ReferenceIdent ident = ((RowCollectExpression)e.getValue()).info().ident();
                builder.put(ident, (RowCollectExpression) e.getValue());
            }
        }
    }

    @Override
    public RowCollectExpression<?, ?> getImplementation(ReferenceInfo info) {
        return implementations.get(info.ident());
    }
}
