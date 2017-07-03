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

package io.crate.operation.reference.sys;

import com.google.common.collect.ImmutableMap;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceImplementation;
import io.crate.metadata.RowCollectExpression;
import io.crate.metadata.RowContextCollectorExpression;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.information.InformationColumnsTableInfo;
import io.crate.metadata.information.InformationPartitionsTableInfo;
import io.crate.metadata.information.InformationRoutinesTableInfo;
import io.crate.metadata.information.InformationSchemataTableInfo;
import io.crate.metadata.information.InformationSqlFeaturesTableInfo;
import io.crate.metadata.information.InformationTableConstraintsTableInfo;
import io.crate.metadata.information.InformationTablesTableInfo;
import io.crate.metadata.pg_catalog.PgCatalogTables;
import io.crate.metadata.pg_catalog.PgTypeTable;
import io.crate.metadata.sys.SysChecksTableInfo;
import io.crate.metadata.sys.SysJobsLogTableInfo;
import io.crate.metadata.sys.SysJobsTableInfo;
import io.crate.metadata.sys.SysNodeChecksTableInfo;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.metadata.sys.SysOperationsLogTableInfo;
import io.crate.metadata.sys.SysOperationsTableInfo;
import io.crate.metadata.sys.SysRepositoriesTableInfo;
import io.crate.metadata.sys.SysShardsTableInfo;
import io.crate.metadata.sys.SysSnapshotsTableInfo;
import io.crate.metadata.sys.SysSummitsTableInfo;
import io.crate.operation.collect.files.SummitsContext;
import io.crate.operation.reference.ReferenceResolver;
import io.crate.operation.reference.information.InformationSchemaExpressionFactories;
import io.crate.operation.reference.sys.check.SysCheck;
import io.crate.operation.reference.sys.check.node.SysNodeCheck;
import io.crate.operation.reference.sys.job.JobContext;
import io.crate.operation.reference.sys.job.JobContextLog;
import io.crate.operation.reference.sys.node.SysNodesExpressionFactories;
import io.crate.operation.reference.sys.operation.OperationContext;
import io.crate.operation.reference.sys.operation.OperationContextLog;
import io.crate.operation.reference.sys.shard.unassigned.UnassignedShardsExpressionFactories;
import io.crate.operation.reference.sys.snapshot.SysSnapshot;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.repositories.Repository;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Singleton
public class RowContextReferenceResolver implements ReferenceResolver<RowCollectExpression<?, ?>> {

    public static final RowContextReferenceResolver INSTANCE = new RowContextReferenceResolver();

    private final Map<TableIdent, Map<ColumnIdent, ? extends RowCollectExpressionFactory>> tableFactories = new HashMap<>();

    /**
     * This is a singleton Use the static INSTANCE attribute
     */
    private RowContextReferenceResolver() {
        tableFactories.put(SysJobsTableInfo.IDENT, getSysJobsExpressions());
        tableFactories.put(SysJobsLogTableInfo.IDENT, getSysJobsLogExpressions());
        tableFactories.put(SysOperationsTableInfo.IDENT, getSysOperationExpressions());
        tableFactories.put(SysOperationsLogTableInfo.IDENT, getSysOperationLogExpressions());
        tableFactories.put(SysChecksTableInfo.IDENT, getSysChecksExpressions());
        tableFactories.put(SysNodeChecksTableInfo.IDENT, getSysNodeChecksExpressions());
        tableFactories.put(SysRepositoriesTableInfo.IDENT, getSysRepositoriesExpressions());
        tableFactories.put(SysSnapshotsTableInfo.IDENT, getSysSnapshotsExpressions());
        tableFactories.put(SysSummitsTableInfo.IDENT, getSummitsExpressions());

        tableFactories.put(InformationSchemataTableInfo.IDENT, InformationSchemaExpressionFactories.schemataFactories());
        tableFactories.put(InformationRoutinesTableInfo.IDENT, InformationSchemaExpressionFactories.routineFactories());
        tableFactories.put(InformationTableConstraintsTableInfo.IDENT, InformationSchemaExpressionFactories.tableConstraintFactories());
        tableFactories.put(InformationPartitionsTableInfo.IDENT, InformationSchemaExpressionFactories.tablePartitionsFactories());
        tableFactories.put(InformationColumnsTableInfo.IDENT, InformationSchemaExpressionFactories.columnsFactories());
        tableFactories.put(InformationTablesTableInfo.IDENT, InformationSchemaExpressionFactories.tablesFactories());
        tableFactories.put(InformationSqlFeaturesTableInfo.IDENT, InformationSchemaExpressionFactories.sqlFeaturesFactories());

        tableFactories.put(SysNodesTableInfo.IDENT, SysNodesExpressionFactories.getNodeStatsContextExpressions());
        tableFactories.put(SysShardsTableInfo.IDENT, UnassignedShardsExpressionFactories.getSysShardsTableInfoFactories());

        tableFactories.put(PgTypeTable.IDENT, PgCatalogTables.pgTypeExpressions());
    }

    public void registerTableFactory(TableIdent ident, Map<ColumnIdent, ? extends RowCollectExpressionFactory> expressionFactories) {
        tableFactories.put(ident, expressionFactories);
    }

    private Map<ColumnIdent, RowCollectExpressionFactory<OperationContextLog>> getSysOperationLogExpressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<OperationContextLog>>builder()
            .put(SysOperationsLogTableInfo.Columns.ID,
                () -> RowContextCollectorExpression.objToBytesRef(OperationContextLog::id))
            .put(SysOperationsLogTableInfo.Columns.JOB_ID,
                () -> RowContextCollectorExpression.objToBytesRef(OperationContextLog::jobId))
            .put(SysOperationsLogTableInfo.Columns.NAME,
                () -> RowContextCollectorExpression.objToBytesRef(OperationContextLog::name))
            .put(SysOperationsLogTableInfo.Columns.STARTED,
                () -> RowContextCollectorExpression.forFunction(OperationContextLog::started))
            .put(SysOperationsLogTableInfo.Columns.USED_BYTES, () -> new RowContextCollectorExpression<OperationContextLog, Long>() {
                @Override
                public Long value() {
                    long usedBytes = row.usedBytes();
                    if (usedBytes == 0) {
                        return null;
                    }
                    return usedBytes;
                }
            })
            .put(SysOperationsLogTableInfo.Columns.ERROR,
                () -> RowContextCollectorExpression.objToBytesRef(OperationContextLog::errorMessage))
            .put(SysOperationsLogTableInfo.Columns.ENDED,
                () -> RowContextCollectorExpression.forFunction(OperationContextLog::ended))
            .build();
    }

    private Map<ColumnIdent, RowCollectExpressionFactory<OperationContext>> getSysOperationExpressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<OperationContext>>builder()
            .put(SysOperationsTableInfo.Columns.ID,
                () -> RowContextCollectorExpression.objToBytesRef(OperationContext::id))
            .put(SysOperationsTableInfo.Columns.JOB_ID,
                () -> RowContextCollectorExpression.objToBytesRef(OperationContext::jobId))
            .put(SysOperationsTableInfo.Columns.NAME,
                () -> RowContextCollectorExpression.objToBytesRef(OperationContext::name))
            .put(SysOperationsTableInfo.Columns.STARTED,
                () -> RowContextCollectorExpression.forFunction(OperationContext::started))
            .put(SysOperationsTableInfo.Columns.USED_BYTES, () -> new RowContextCollectorExpression<OperationContext, Long>() {
                @Override
                public Long value() {
                    if (row.usedBytes == 0) {
                        return null;
                    }
                    return row.usedBytes;
                }
            })
            .build();
    }

    private ImmutableMap<ColumnIdent, RowCollectExpressionFactory<JobContextLog>> getSysJobsLogExpressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<JobContextLog>>builder()
            .put(SysJobsLogTableInfo.Columns.ID,
                () -> RowContextCollectorExpression.objToBytesRef(JobContextLog::id))
            .put(SysJobsTableInfo.Columns.USERNAME,
                () -> RowContextCollectorExpression.objToBytesRef(JobContextLog::username))
            .put(SysJobsLogTableInfo.Columns.STMT,
                () -> RowContextCollectorExpression.objToBytesRef(JobContextLog::statement))
            .put(SysJobsLogTableInfo.Columns.STARTED,
                () -> RowContextCollectorExpression.forFunction(JobContextLog::started))
            .put(SysJobsLogTableInfo.Columns.ENDED,
                () -> RowContextCollectorExpression.forFunction(JobContextLog::ended))
            .put(SysJobsLogTableInfo.Columns.ERROR,
                () -> RowContextCollectorExpression.objToBytesRef(JobContextLog::errorMessage))
            .build();
    }

    private ImmutableMap<ColumnIdent, RowCollectExpressionFactory<JobContext>> getSysJobsExpressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<JobContext>>builder()
            .put(SysJobsTableInfo.Columns.ID,
                () -> RowContextCollectorExpression.objToBytesRef(JobContext::id))
            .put(SysJobsTableInfo.Columns.USERNAME,
                () -> RowContextCollectorExpression.objToBytesRef(JobContext::username))
            .put(SysJobsTableInfo.Columns.STMT,
                () -> RowContextCollectorExpression.objToBytesRef(JobContext::stmt))
            .put(SysJobsTableInfo.Columns.STARTED,
                () -> RowContextCollectorExpression.forFunction(JobContext::started))
            .build();
    }

    private ImmutableMap<ColumnIdent, RowCollectExpressionFactory<SysCheck>> getSysChecksExpressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<SysCheck>>builder()
            .put(SysChecksTableInfo.Columns.ID,
                () -> RowContextCollectorExpression.forFunction(SysCheck::id))
            .put(SysChecksTableInfo.Columns.DESCRIPTION,
                () -> RowContextCollectorExpression.forFunction(SysCheck::description))
            .put(SysChecksTableInfo.Columns.SEVERITY,
                () -> RowContextCollectorExpression.forFunction((SysCheck r) -> r.severity().value()))
            .put(SysChecksTableInfo.Columns.PASSED,
                () -> RowContextCollectorExpression.forFunction(SysCheck::validate))
            .build();
    }

    private ImmutableMap<ColumnIdent, RowCollectExpressionFactory<SysNodeCheck>> getSysNodeChecksExpressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<SysNodeCheck>>builder()
            .put(SysNodeChecksTableInfo.Columns.ID,
                () -> RowContextCollectorExpression.forFunction(SysNodeCheck::id))
            .put(SysNodeChecksTableInfo.Columns.NODE_ID,
                () -> RowContextCollectorExpression.forFunction(SysNodeCheck::nodeId))
            .put(SysNodeChecksTableInfo.Columns.DESCRIPTION,
                () -> RowContextCollectorExpression.forFunction(SysNodeCheck::description))
            .put(SysNodeChecksTableInfo.Columns.SEVERITY,
                () -> RowContextCollectorExpression.forFunction((SysNodeCheck x) -> x.severity().value()))
            .put(SysNodeChecksTableInfo.Columns.PASSED,
                () -> RowContextCollectorExpression.forFunction(SysNodeCheck::validate))
            .put(SysNodeChecksTableInfo.Columns.ACKNOWLEDGED,
                () -> RowContextCollectorExpression.forFunction(SysNodeCheck::acknowledged))
            .put(DocSysColumns.ID,
                () -> RowContextCollectorExpression.forFunction(SysNodeCheck::rowId))
            .build();
    }

    private ImmutableMap<ColumnIdent, RowCollectExpressionFactory<Repository>> getSysRepositoriesExpressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<Repository>>builder()
            .put(SysRepositoriesTableInfo.Columns.NAME,
                () -> RowContextCollectorExpression.objToBytesRef((Repository r) -> r.getMetadata().name()))
            .put(SysRepositoriesTableInfo.Columns.TYPE,
                () -> RowContextCollectorExpression.objToBytesRef((Repository r) -> r.getMetadata().type()))
            .put(SysRepositoriesTableInfo.Columns.SETTINGS,
                () -> RowContextCollectorExpression
                    .forFunction((Repository r) -> r.getMetadata().settings().getAsStructuredMap()))
            .build();
    }

    private ImmutableMap<ColumnIdent, RowCollectExpressionFactory<SysSnapshot>> getSysSnapshotsExpressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<SysSnapshot>>builder()
            .put(SysSnapshotsTableInfo.Columns.NAME,
                () -> RowContextCollectorExpression.objToBytesRef(SysSnapshot::name))
            .put(SysSnapshotsTableInfo.Columns.REPOSITORY,
                () -> RowContextCollectorExpression.objToBytesRef(SysSnapshot::repository))
            .put(SysSnapshotsTableInfo.Columns.CONCRETE_INDICES,
                () -> RowContextCollectorExpression.forFunction((SysSnapshot s) -> {
                    List<String> concreteIndices = s.concreteIndices();
                    BytesRef[] indices = new BytesRef[concreteIndices.size()];
                    for (int i = 0; i < concreteIndices.size(); i++) {
                        indices[i] = BytesRefs.toBytesRef(concreteIndices.get(i));
                    }
                    return indices;
                }))
            .put(SysSnapshotsTableInfo.Columns.STARTED,
                () -> RowContextCollectorExpression.forFunction(SysSnapshot::started))
            .put(SysSnapshotsTableInfo.Columns.FINISHED,
                () -> RowContextCollectorExpression.forFunction(SysSnapshot::finished))
            .put(SysSnapshotsTableInfo.Columns.VERSION,
                () -> RowContextCollectorExpression.objToBytesRef(SysSnapshot::version))
            .put(SysSnapshotsTableInfo.Columns.STATE,
                () -> RowContextCollectorExpression.objToBytesRef(SysSnapshot::state))
            .build();
    }

    private static Map<ColumnIdent, RowCollectExpressionFactory<SummitsContext>> getSummitsExpressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<SummitsContext>>builder()
            .put(SysSummitsTableInfo.Columns.MOUNTAIN,
                () -> RowContextCollectorExpression.objToBytesRef(SummitsContext::mountain))
            .put(SysSummitsTableInfo.Columns.HEIGHT,
                () -> RowContextCollectorExpression.forFunction(SummitsContext::height))
            .put(SysSummitsTableInfo.Columns.PROMINENCE,
                () -> RowContextCollectorExpression.forFunction(SummitsContext::prominence))
            .put(SysSummitsTableInfo.Columns.COORDINATES,
                () -> RowContextCollectorExpression.forFunction(SummitsContext::coordinates))
            .put(SysSummitsTableInfo.Columns.RANGE,
                () -> RowContextCollectorExpression.objToBytesRef(SummitsContext::range))
            .put(SysSummitsTableInfo.Columns.CLASSIFICATION,
                () -> RowContextCollectorExpression.objToBytesRef(SummitsContext::classification))
            .put(SysSummitsTableInfo.Columns.REGION,
                () -> RowContextCollectorExpression.objToBytesRef(SummitsContext::region))
            .put(SysSummitsTableInfo.Columns.COUNTRY,
                () -> RowContextCollectorExpression.objToBytesRef(SummitsContext::country))
            .put(SysSummitsTableInfo.Columns.FIRST_ASCENT,
                () -> RowContextCollectorExpression.forFunction(SummitsContext::firstAscent))
            .build();
    }

    @Override
    public RowCollectExpression<?, ?> getImplementation(Reference refInfo) {
        return rowCollectExpressionFromFactoryMap(tableFactories, refInfo);
    }

    private static RowCollectExpression<?, ?> rowCollectExpressionFromFactoryMap(
        Map<TableIdent, Map<ColumnIdent, ? extends RowCollectExpressionFactory>> factoryMap,
        Reference info) {

        Map<ColumnIdent, ? extends RowCollectExpressionFactory> innerFactories = factoryMap.get(info.ident().tableIdent());
        if (innerFactories == null) {
            return null;
        }
        ColumnIdent columnIdent = info.ident().columnIdent();
        RowCollectExpressionFactory factory = innerFactories.get(columnIdent);
        if (factory != null) {
            return factory.create();
        }
        if (columnIdent.isColumn()) {
            return null;
        }
        return getImplementationByRootTraversal(innerFactories, columnIdent);
    }

    private static RowCollectExpression<?, ?> getImplementationByRootTraversal(
        Map<ColumnIdent, ? extends RowCollectExpressionFactory> innerFactories,
        ColumnIdent columnIdent) {

        RowCollectExpressionFactory factory = innerFactories.get(columnIdent.getRoot());
        if (factory == null) {
            return null;
        }
        ReferenceImplementation refImpl = factory.create();
        return (RowCollectExpression<?, ?>) ReferenceImplementation.getChildByPath(refImpl, columnIdent.path());
    }
}
