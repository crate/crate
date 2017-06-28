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
            .put(SysOperationsLogTableInfo.Columns.ID, () -> new RowContextCollectorExpression<OperationContextLog, BytesRef>() {
                @Override
                public BytesRef value() {
                    return BytesRefs.toBytesRef(row.id());
                }
            })
            .put(SysOperationsLogTableInfo.Columns.JOB_ID, () -> new RowContextCollectorExpression<OperationContextLog, BytesRef>() {
                @Override
                public BytesRef value() {
                    return BytesRefs.toBytesRef(row.jobId());
                }
            })
            .put(SysOperationsLogTableInfo.Columns.NAME, () -> new RowContextCollectorExpression<OperationContextLog, BytesRef>() {
                @Override
                public BytesRef value() {
                    return new BytesRef(row.name());
                }
            })
            .put(SysOperationsLogTableInfo.Columns.STARTED, () -> new RowContextCollectorExpression<OperationContextLog, Long>() {
                @Override
                public Long value() {
                    return row.started();
                }
            })
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
            .put(SysOperationsLogTableInfo.Columns.ERROR, () -> new RowContextCollectorExpression<OperationContextLog, BytesRef>() {
                @Override
                public BytesRef value() {
                    return BytesRefs.toBytesRef(row.errorMessage());
                }
            })
            .put(SysOperationsLogTableInfo.Columns.ENDED, () -> new RowContextCollectorExpression<OperationContextLog, Long>() {
                @Override
                public Long value() {
                    return row.ended();
                }
            })
            .build();
    }

    private Map<ColumnIdent, RowCollectExpressionFactory<OperationContext>> getSysOperationExpressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<OperationContext>>builder()
            .put(SysOperationsTableInfo.Columns.ID, () -> new RowContextCollectorExpression<OperationContext, BytesRef>() {
                @Override
                public BytesRef value() {
                    return BytesRefs.toBytesRef(row.id);
                }
            })
            .put(SysOperationsTableInfo.Columns.JOB_ID, () -> new RowContextCollectorExpression<OperationContext, BytesRef>() {
                @Override
                public BytesRef value() {
                    return BytesRefs.toBytesRef(row.jobId);
                }
            })
            .put(SysOperationsTableInfo.Columns.NAME, () -> new RowContextCollectorExpression<OperationContext, BytesRef>() {
                @Override
                public BytesRef value() {
                    return new BytesRef(row.name);
                }
            })
            .put(SysOperationsTableInfo.Columns.STARTED, () -> new RowContextCollectorExpression<OperationContext, Long>() {
                @Override
                public Long value() {
                    return row.started;
                }
            })
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
            .put(SysJobsLogTableInfo.Columns.ID, () -> new RowContextCollectorExpression<JobContextLog, BytesRef>() {
                @Override
                public BytesRef value() {
                    return new BytesRef(row.id().toString());
                }
            })
            .put(SysJobsLogTableInfo.Columns.STMT, () -> new RowContextCollectorExpression<JobContextLog, BytesRef>() {
                @Override
                public BytesRef value() {
                    return new BytesRef(row.statement());
                }
            })
            .put(SysJobsLogTableInfo.Columns.STARTED, () -> new RowContextCollectorExpression<JobContextLog, Long>() {
                @Override
                public Long value() {
                    return row.started();
                }
            })
            .put(SysJobsLogTableInfo.Columns.ENDED, () -> new RowContextCollectorExpression<JobContextLog, Long>() {
                @Override
                public Long value() {
                    return row.ended();
                }
            })
            .put(SysJobsLogTableInfo.Columns.ERROR, () -> new RowContextCollectorExpression<JobContextLog, BytesRef>() {
                @Override
                public BytesRef value() {
                    String err = row.errorMessage();
                    if (err == null) {
                        return null;
                    }
                    return new BytesRef(err);
                }
            })
            .build();
    }

    private ImmutableMap<ColumnIdent, RowCollectExpressionFactory<JobContext>> getSysJobsExpressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<JobContext>>builder()
            .put(SysJobsTableInfo.Columns.ID, () -> new RowContextCollectorExpression<JobContext, BytesRef>() {
                @Override
                public BytesRef value() {
                    return BytesRefs.toBytesRef(row.id);
                }
            })
            .put(SysJobsTableInfo.Columns.STMT, () -> new RowContextCollectorExpression<JobContext, BytesRef>() {
                @Override
                public BytesRef value() {
                    return new BytesRef(row.stmt);
                }
            })
            .put(SysJobsTableInfo.Columns.STARTED, () -> new RowContextCollectorExpression<JobContext, Long>() {
                @Override
                public Long value() {
                    return row.started;
                }
            })
            .build();
    }

    private ImmutableMap<ColumnIdent, RowCollectExpressionFactory<SysCheck>> getSysChecksExpressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<SysCheck>>builder()
            .put(SysChecksTableInfo.Columns.ID, () -> new RowContextCollectorExpression<SysCheck, Integer>() {
                @Override
                public Integer value() {
                    return row.id();
                }
            })
            .put(SysChecksTableInfo.Columns.DESCRIPTION, () -> new RowContextCollectorExpression<SysCheck, BytesRef>() {
                @Override
                public BytesRef value() {
                    return row.description();
                }
            })
            .put(SysChecksTableInfo.Columns.SEVERITY, () -> new RowContextCollectorExpression<SysCheck, Integer>() {
                @Override
                public Integer value() {
                    return row.severity().value();
                }
            })
            .put(SysChecksTableInfo.Columns.PASSED, () -> new RowContextCollectorExpression<SysCheck, Boolean>() {

                @Override
                public Boolean value() {
                    return row.validate();
                }
            })
            .build();
    }

    private ImmutableMap<ColumnIdent, RowCollectExpressionFactory<SysNodeCheck>> getSysNodeChecksExpressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<SysNodeCheck>>builder()
            .put(SysNodeChecksTableInfo.Columns.ID, () -> new RowContextCollectorExpression<SysNodeCheck, Integer>() {
                @Override
                public Integer value() {
                    return row.id();
                }
            })
            .put(SysNodeChecksTableInfo.Columns.NODE_ID, () -> new RowContextCollectorExpression<SysNodeCheck, BytesRef>() {
                @Override
                public BytesRef value() {
                    return row.nodeId();
                }
            })
            .put(SysNodeChecksTableInfo.Columns.DESCRIPTION, () -> new RowContextCollectorExpression<SysNodeCheck, BytesRef>() {
                @Override
                public BytesRef value() {
                    return row.description();
                }
            })
            .put(SysNodeChecksTableInfo.Columns.SEVERITY, () -> new RowContextCollectorExpression<SysNodeCheck, Integer>() {
                @Override
                public Integer value() {
                    return row.severity().value();
                }
            })
            .put(SysNodeChecksTableInfo.Columns.PASSED, () -> new RowContextCollectorExpression<SysNodeCheck, Boolean>() {
                @Override
                public Boolean value() {
                    return row.validate();
                }
            })
            .put(SysNodeChecksTableInfo.Columns.ACKNOWLEDGED, () -> new RowContextCollectorExpression<SysNodeCheck, Boolean>() {
                @Override
                public Boolean value() {
                    return row.acknowledged();
                }
            })
            .put(DocSysColumns.ID, () -> new RowContextCollectorExpression<SysNodeCheck, BytesRef>() {
                @Override
                public BytesRef value() {
                    return row.rowId();
                }
            })
            .build();
    }

    private ImmutableMap<ColumnIdent, RowCollectExpressionFactory<Repository>> getSysRepositoriesExpressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<Repository>>builder()
            .put(SysRepositoriesTableInfo.Columns.NAME, () -> new RowContextCollectorExpression<Repository, BytesRef>() {
                @Override
                public BytesRef value() {
                    return new BytesRef(row.getMetadata().name());
                }
            })
            .put(SysRepositoriesTableInfo.Columns.TYPE, () -> new RowContextCollectorExpression<Repository, BytesRef>() {
                @Override
                public BytesRef value() {
                    return new BytesRef(row.getMetadata().type());
                }
            })
            .put(SysRepositoriesTableInfo.Columns.SETTINGS, () -> new RowContextCollectorExpression<Repository, Map<String, Object>>() {
                @Override
                public Map<String, Object> value() {
                    return row.getMetadata().settings().getAsStructuredMap();
                }
            })
            .build();
    }

    private ImmutableMap<ColumnIdent, RowCollectExpressionFactory<SysSnapshot>> getSysSnapshotsExpressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<SysSnapshot>>builder()
            .put(SysSnapshotsTableInfo.Columns.NAME, () -> new RowContextCollectorExpression<SysSnapshot, BytesRef>() {
                @Override
                public BytesRef value() {
                    return new BytesRef(row.name());
                }
            })
            .put(SysSnapshotsTableInfo.Columns.REPOSITORY, () -> new RowContextCollectorExpression<SysSnapshot, BytesRef>() {
                @Override
                public BytesRef value() {
                    return new BytesRef(row.repository());
                }
            })
            .put(SysSnapshotsTableInfo.Columns.CONCRETE_INDICES, () -> new RowContextCollectorExpression<SysSnapshot, BytesRef[]>() {
                @Override
                public BytesRef[] value() {
                    List<String> concreteIndices = row.concreteIndices();
                    BytesRef[] indices = new BytesRef[concreteIndices.size()];
                    for (int i = 0; i < concreteIndices.size(); i++) {
                        indices[i] = BytesRefs.toBytesRef(concreteIndices.get(i));
                    }
                    return indices;
                }
            })
            .put(SysSnapshotsTableInfo.Columns.STARTED, () -> new RowContextCollectorExpression<SysSnapshot, Long>() {
                @Override
                public Long value() {
                    return row.started();
                }
            })
            .put(SysSnapshotsTableInfo.Columns.FINISHED, () -> new RowContextCollectorExpression<SysSnapshot, Long>() {
                @Override
                public Long value() {
                    return row.finished();
                }
            })
            .put(SysSnapshotsTableInfo.Columns.VERSION, () -> new RowContextCollectorExpression<SysSnapshot, BytesRef>() {
                @Override
                public BytesRef value() {
                    return new BytesRef(row.version());
                }
            })
            .put(SysSnapshotsTableInfo.Columns.STATE, () -> new RowContextCollectorExpression<SysSnapshot, BytesRef>() {
                @Override
                public BytesRef value() {
                    return new BytesRef(row.state());
                }
            })
            .build();
    }

    private static Map<ColumnIdent, RowCollectExpressionFactory<SummitsContext>> getSummitsExpressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<SummitsContext>>builder()
            .put(SysSummitsTableInfo.Columns.MOUNTAIN, () -> new RowContextCollectorExpression<SummitsContext, BytesRef>() {
                @Override
                public BytesRef value() {
                    return BytesRefs.toBytesRef(row.mountain);
                }
            })
            .put(SysSummitsTableInfo.Columns.HEIGHT, () -> new RowContextCollectorExpression<SummitsContext, Integer>() {
                @Override
                public Integer value() {
                    return row.height;
                }
            })
            .put(SysSummitsTableInfo.Columns.PROMINENCE, () -> new RowContextCollectorExpression<SummitsContext, Integer>() {
                @Override
                public Integer value() {
                    return row.prominence;
                }
            })
            .put(SysSummitsTableInfo.Columns.COORDINATES, () -> new RowContextCollectorExpression<SummitsContext, Double[]>() {
                @Override
                public Double[] value() {
                    return row.coordinates;
                }
            })
            .put(SysSummitsTableInfo.Columns.RANGE, () -> new RowContextCollectorExpression<SummitsContext, BytesRef>() {
                @Override
                public BytesRef value() {
                    return BytesRefs.toBytesRef(row.range);
                }
            })
            .put(SysSummitsTableInfo.Columns.CLASSIFICATION, () -> new RowContextCollectorExpression<SummitsContext, BytesRef>() {
                @Override
                public BytesRef value() {
                    return BytesRefs.toBytesRef(row.classification);
                }
            })
            .put(SysSummitsTableInfo.Columns.REGION, () -> new RowContextCollectorExpression<SummitsContext, BytesRef>() {
                @Override
                public BytesRef value() {
                    return BytesRefs.toBytesRef(row.region);
                }
            })
            .put(SysSummitsTableInfo.Columns.COUNTRY, () -> new RowContextCollectorExpression<SummitsContext, BytesRef>() {
                @Override
                public BytesRef value() {
                    return BytesRefs.toBytesRef(row.country);
                }
            })
            .put(SysSummitsTableInfo.Columns.FIRST_ASCENT, () -> new RowContextCollectorExpression<SummitsContext, Integer>() {
                @Override
                public Integer value() {
                    return row.firstAscent;
                }
            }).build();
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
