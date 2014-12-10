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
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.information.RowCollectExpression;
import io.crate.operation.reference.DocLevelReferenceResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

@Singleton
public class InformationDocLevelReferenceResolver implements DocLevelReferenceResolver<RowCollectExpression<?, ?>> {

    private final ImmutableMap<ReferenceIdent, RowCollectExpression<?, ?>> implementations;

    @Inject
    public InformationDocLevelReferenceResolver() {
        ImmutableMap.Builder<ReferenceIdent, RowCollectExpression<?, ?>> builder =
                ImmutableMap.builder();

        // information_schema.tables
        add(builder, InformationTablesExpression.SCHEMA_NAME_EXPRESSION);
        add(builder, InformationTablesExpression.TABLE_NAME_EXPRESSION);
        add(builder, InformationTablesExpression.NUMBER_OF_SHARDS_EXPRESSION);
        add(builder, InformationTablesExpression.NUMBER_OF_REPLICAS_EXPRESSION);
        add(builder, InformationTablesExpression.CLUSTERED_BY_EXPRESSION);
        add(builder, InformationTablesExpression.PARTITION_BY_EXPRESSION);
        add(builder, InformationTablesExpression.BLOB_PATH_EXPRESSION);

                // information_schema.columns
        add(builder, InformationColumnsExpression.SCHEMA_NAME_EXPRESSION);
        add(builder, InformationColumnsExpression.TABLE_NAME_EXPRESSION);
        add(builder, InformationColumnsExpression.COLUMN_NAME_EXPRESSION);
        add(builder, InformationColumnsExpression.ORDINAL_EXPRESSION);
        add(builder, InformationColumnsExpression.DATA_TYPE_EXPRESSION);

        // information_schema.table_partitions
        add(builder, InformationTablePartitionsExpression.TABLE_NAME_EXPRESSION);
        add(builder, InformationTablePartitionsExpression.SCHEMA_NAME_EXPRESSION);
        add(builder, InformationTablePartitionsExpression.PARTITION_IDENT_EXPRESSION);
        add(builder, InformationTablePartitionsExpression.VALUES_EXPRESSION);

        // information_schema.table_constraints
        add(builder, InformationTableConstraintsExpression.SCHEMA_NAME_EXPRESSION);
        add(builder, InformationTableConstraintsExpression.TABLE_NAME_EXPRESSION);
        add(builder, InformationTableConstraintsExpression.CONSTRAINT_NAME_EXPRESSION);
        add(builder, InformationTableConstraintsExpression.CONSTRAINT_TYPE_EXPRESSION);

        // information_schema.routines
        add(builder, InformationRoutinesExpression.ROUTINE_NAME_EXPRESSION);
        add(builder, InformationRoutinesExpression.ROUTINE_TYPE_EXPRESSION);

        // information_schema.schemata
        add(builder, InformationSchemataExpression.SCHEMA_NAME_EXPRESSION);

        implementations = builder.build();
    }

    private static void add(ImmutableMap.Builder<ReferenceIdent, RowCollectExpression<?, ?>> builder,
                            RowCollectExpression<?, ?> expression) {
        builder.put(expression.info().ident(), expression);
    }

    @Override
    public RowCollectExpression<?, ?> getImplementation(ReferenceInfo info) {
        return implementations.get(info.ident());
    }
}
