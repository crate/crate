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

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.crate.metadata.*;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.expressions.WriteableRowContextExpression;
import io.crate.metadata.information.*;
import io.crate.metadata.pg_catalog.PgCatalogTables;
import io.crate.metadata.pg_catalog.PgTypeTable;
import io.crate.metadata.sys.*;
import io.crate.operation.reference.ReferenceResolver;
import io.crate.operation.reference.information.InformationSchemaExpressionFactories;
import io.crate.operation.reference.sys.check.SysCheck;
import io.crate.operation.reference.sys.check.SysNodeCheck;
import io.crate.operation.reference.sys.job.JobContext;
import io.crate.operation.reference.sys.job.JobContextLog;
import io.crate.operation.reference.sys.node.SysNodesExpressionFactories;
import io.crate.operation.reference.sys.operation.OperationContext;
import io.crate.operation.reference.sys.operation.OperationContextLog;
import io.crate.operation.reference.sys.repositories.SysRepository;
import io.crate.operation.reference.sys.shard.unassigned.UnassignedShardsExpressionFactories;
import io.crate.operation.reference.sys.snapshot.SysSnapshot;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.lucene.BytesRefs;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

@Singleton
public class RowContextReferenceResolver implements ReferenceResolver<RowCollectExpression<?, ?>> {

    public static final RowContextReferenceResolver INSTANCE = new RowContextReferenceResolver();

    private final Map<TableIdent, Map<ColumnIdent, RowCollectExpressionFactory>> tableFactories = new HashMap<>();

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

        tableFactories.put(InformationSchemataTableInfo.IDENT, InformationSchemaExpressionFactories.schemataFactories());
        tableFactories.put(InformationRoutinesTableInfo.IDENT, InformationSchemaExpressionFactories.routineFactories());
        tableFactories.put(InformationTableConstraintsTableInfo.IDENT, InformationSchemaExpressionFactories.tableConstraintFactories());
        tableFactories.put(InformationPartitionsTableInfo.IDENT, InformationSchemaExpressionFactories.tablePartitionsFactories());
        tableFactories.put(InformationColumnsTableInfo.IDENT, InformationSchemaExpressionFactories.columnsFactories());
        tableFactories.put(InformationTablesTableInfo.IDENT, InformationSchemaExpressionFactories.tablesFactories());
        tableFactories.put(InformationSqlFeaturesTableInfo.IDENT, InformationSchemaExpressionFactories.sqlFeaturesFactories());

        tableFactories.put(SysNodesTableInfo.IDENT, SysNodesExpressionFactories.getSysNodesTableInfoFactories());
        tableFactories.put(SysShardsTableInfo.IDENT, UnassignedShardsExpressionFactories.getSysShardsTableInfoFactories());

        tableFactories.put(PgTypeTable.IDENT, PgCatalogTables.pgTypeExpressions());
    }

    private Map<ColumnIdent, RowCollectExpressionFactory> getSysOperationLogExpressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory>builder()
                .put(SysOperationsLogTableInfo.Columns.ID, new RowCollectExpressionFactory() {
                    @Override
                    public RowCollectExpression create() {
                        return new RowContextCollectorExpression<OperationContextLog, BytesRef>() {
                            @Override
                            public BytesRef value() {
                                return BytesRefs.toBytesRef(row.id());
                            }
                        };
                    }
                })
                .put(SysOperationsLogTableInfo.Columns.JOB_ID, new RowCollectExpressionFactory() {
                    @Override
                    public RowCollectExpression create() {
                        return new RowContextCollectorExpression<OperationContextLog, BytesRef>() {
                            @Override
                            public BytesRef value() {
                                return BytesRefs.toBytesRef(row.jobId());
                            }
                        };
                    }
                })
                .put(SysOperationsLogTableInfo.Columns.NAME, new RowCollectExpressionFactory() {
                    @Override
                    public RowCollectExpression create() {
                        return new RowContextCollectorExpression<OperationContextLog, BytesRef>() {
                            @Override
                            public BytesRef value() {
                                return new BytesRef(row.name());
                            }
                        };
                    }
                })
                .put(SysOperationsLogTableInfo.Columns.STARTED, new RowCollectExpressionFactory() {
                    @Override
                    public RowCollectExpression create() {
                        return new RowContextCollectorExpression<OperationContextLog, Long>() {
                            @Override
                            public Long value() {
                                return row.started();
                            }
                        };
                    }
                })
                .put(SysOperationsLogTableInfo.Columns.USED_BYTES, new RowCollectExpressionFactory() {
                    @Override
                    public RowCollectExpression create() {
                        return new RowContextCollectorExpression<OperationContextLog, Long>() {
                            @Override
                            public Long value() {
                                long usedBytes = row.usedBytes();
                                if (usedBytes == 0) {
                                    return null;
                                }
                                return usedBytes;
                            }
                        };
                    }
                })
                .put(SysOperationsLogTableInfo.Columns.ERROR, new RowCollectExpressionFactory() {
                    @Override
                    public RowCollectExpression create() {
                        return new RowContextCollectorExpression<OperationContextLog, BytesRef>() {
                            @Override
                            public BytesRef value() {
                                return BytesRefs.toBytesRef(row.errorMessage());
                            }
                        };
                    }
                })
                .put(SysOperationsLogTableInfo.Columns.ENDED, new RowCollectExpressionFactory() {
                    @Override
                    public RowCollectExpression create() {
                        return new RowContextCollectorExpression<OperationContextLog, Long>() {
                            @Override
                            public Long value() {
                                return row.ended();
                            }
                        };
                    }
                })
                .build();
    }

    private Map<ColumnIdent, RowCollectExpressionFactory> getSysOperationExpressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory>builder()
                .put(SysOperationsTableInfo.Columns.ID, new RowCollectExpressionFactory() {
                    @Override
                    public RowCollectExpression create() {
                        return new RowContextCollectorExpression<OperationContext, BytesRef>() {
                            @Override
                            public BytesRef value() {
                                return BytesRefs.toBytesRef(row.id);
                            }
                        };
                    }
                })
                .put(SysOperationsTableInfo.Columns.JOB_ID, new RowCollectExpressionFactory() {
                    @Override
                    public RowCollectExpression create() {
                        return new RowContextCollectorExpression<OperationContext, BytesRef>() {
                            @Override
                            public BytesRef value() {
                                return BytesRefs.toBytesRef(row.jobId);
                            }
                        };
                    }
                })
                .put(SysOperationsTableInfo.Columns.NAME, new RowCollectExpressionFactory() {
                    @Override
                    public RowCollectExpression create() {
                        return new RowContextCollectorExpression<OperationContext, BytesRef>() {
                            @Override
                            public BytesRef value() {
                                return new BytesRef(row.name);
                            }
                        };
                    }
                })
                .put(SysOperationsTableInfo.Columns.STARTED, new RowCollectExpressionFactory() {
                    @Override
                    public RowCollectExpression create() {
                        return new RowContextCollectorExpression<OperationContext, Long>() {
                            @Override
                            public Long value() {
                                return row.started;
                            }
                        };
                    }
                })
                .put(SysOperationsTableInfo.Columns.USED_BYTES, new RowCollectExpressionFactory() {
                    @Override
                    public RowCollectExpression create() {
                        return new RowContextCollectorExpression<OperationContext, Long>() {
                            @Override
                            public Long value() {
                                if (row.usedBytes == 0) {
                                    return null;
                                }
                                return row.usedBytes;
                            }
                        };
                    }
                })
                .build();
    }

    private ImmutableMap<ColumnIdent, RowCollectExpressionFactory> getSysJobsLogExpressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory>builder()
                .put(SysJobsLogTableInfo.Columns.ID, new RowCollectExpressionFactory() {
                    @Override
                    public RowCollectExpression create() {
                        return new RowContextCollectorExpression<JobContextLog, BytesRef>() {
                            @Override
                            public BytesRef value() {
                                return new BytesRef(row.id().toString());
                            }
                        };
                    }
                })
                .put(SysJobsLogTableInfo.Columns.STMT, new RowCollectExpressionFactory() {
                    @Override
                    public RowCollectExpression create() {
                        return new RowContextCollectorExpression<JobContextLog, BytesRef>() {
                            @Override
                            public BytesRef value() {
                                return new BytesRef(row.statement());
                            }
                        };
                    }
                })
                .put(SysJobsLogTableInfo.Columns.STARTED, new RowCollectExpressionFactory() {
                    @Override
                    public RowCollectExpression create() {
                        return new RowContextCollectorExpression<JobContextLog, Long>() {
                            @Override
                            public Long value() {
                                return row.started();
                            }
                        };
                    }
                })
                .put(SysJobsLogTableInfo.Columns.ENDED, new RowCollectExpressionFactory() {
                    @Override
                    public RowCollectExpression create() {
                        return new RowContextCollectorExpression<JobContextLog, Long>() {
                            @Override
                            public Long value() {
                                return row.ended();
                            }
                        };
                    }
                })
                .put(SysJobsLogTableInfo.Columns.ERROR, new RowCollectExpressionFactory() {
                    @Override
                    public RowCollectExpression create() {
                        return new RowContextCollectorExpression<JobContextLog, BytesRef>() {
                            @Override
                            public BytesRef value() {
                                String err = row.errorMessage();
                                if (err == null) {
                                    return null;
                                }
                                return new BytesRef(err);
                            }
                        };
                    }
                })
                .build();
    }

    private ImmutableMap<ColumnIdent, RowCollectExpressionFactory> getSysJobsExpressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory>builder()
                .put(SysJobsTableInfo.Columns.ID, new RowCollectExpressionFactory() {
                    @Override
                    public RowCollectExpression create() {
                        return new RowContextCollectorExpression<JobContext, BytesRef>() {
                            @Override
                            public BytesRef value() {
                                return BytesRefs.toBytesRef(row.id);
                            }
                        };
                    }
                })
                .put(SysJobsTableInfo.Columns.STMT, new RowCollectExpressionFactory() {
                    @Override
                    public RowCollectExpression create() {
                        return new RowContextCollectorExpression<JobContext, BytesRef>() {
                            @Override
                            public BytesRef value() {
                                return new BytesRef(row.stmt);
                            }
                        };
                    }
                })
                .put(SysJobsTableInfo.Columns.STARTED, new RowCollectExpressionFactory() {
                    @Override
                    public RowCollectExpression create() {
                        return new RowContextCollectorExpression<JobContext, Long>() {
                            @Override
                            public Long value() {
                                return row.started;
                            }
                        };
                    }
                })
                .build();
    }

    private ImmutableMap<ColumnIdent, RowCollectExpressionFactory> getSysChecksExpressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory>builder()
                .put(SysChecksTableInfo.Columns.ID, new RowCollectExpressionFactory() {
                    @Override
                    public RowCollectExpression create() {
                        return new RowContextCollectorExpression<SysCheck, Integer>() {
                            @Override
                            public Integer value() {
                                return row.id();
                            }
                        };
                    }
                })
                .put(SysChecksTableInfo.Columns.DESCRIPTION, new RowCollectExpressionFactory() {
                    @Override
                    public RowCollectExpression create() {
                        return new RowContextCollectorExpression<SysCheck, BytesRef>() {
                            @Override
                            public BytesRef value() {
                                return row.description();
                            }
                        };
                    }
                })
                .put(SysChecksTableInfo.Columns.SEVERITY, new RowCollectExpressionFactory() {
                    @Override
                    public RowCollectExpression create() {
                        return new RowContextCollectorExpression<SysCheck, Integer>() {
                            @Override
                            public Integer value() {
                                return row.severity().value();
                            }
                        };
                    }
                })
                .put(SysChecksTableInfo.Columns.PASSED, new RowCollectExpressionFactory() {
                    @Override
                    public RowCollectExpression create() {
                        return new RowContextCollectorExpression<SysCheck, Boolean>() {

                            @Override
                            public Boolean value() {
                                return row.validate();
                            }
                        };
                    }
                })
                .build();
    }

    private ImmutableMap<ColumnIdent,RowCollectExpressionFactory> getSysNodeChecksExpressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory>builder()
            .put(SysNodeChecksTableInfo.Columns.ID, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new RowContextCollectorExpression<SysNodeCheck, Integer>() {
                        @Override
                        public Integer value() {
                            return row.id();
                        }
                    };
                }
            })
            .put(SysNodeChecksTableInfo.Columns.NODE_ID, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new RowContextCollectorExpression<SysNodeCheck, BytesRef>() {
                        @Override
                        public BytesRef value() {
                            return row.nodeId();
                        }
                    };
                }
            })
            .put(SysNodeChecksTableInfo.Columns.DESCRIPTION, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new RowContextCollectorExpression<SysNodeCheck, BytesRef>() {
                        @Override
                        public BytesRef value() {
                            return row.description();
                        }
                    };
                }
            })
            .put(SysNodeChecksTableInfo.Columns.SEVERITY, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new RowContextCollectorExpression<SysNodeCheck, Integer>() {
                        @Override
                        public Integer value() {
                            return row.severity().value();
                        }
                    };
                }
            })
            .put(SysNodeChecksTableInfo.Columns.PASSED, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new RowContextCollectorExpression<SysNodeCheck, Boolean>() {
                        @Override
                        public Boolean value() {
                            return row.validate();
                        }
                    };
                }
            })
            .put(SysNodeChecksTableInfo.Columns.ACKNOWLEDGED, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new WriteableRowContextExpression<SysNodeCheck, Boolean, Boolean>() {
                        @Override
                        public void updateValue(SysNodeCheck sysCheck, Boolean value) {
                            sysCheck.acknowledged(value);
                        }

                        @Override
                        public Boolean value() {
                            return row.acknowledged();
                        }
                    };
                }
            })
            .build();
    }

    private ImmutableMap<ColumnIdent, RowCollectExpressionFactory> getSysRepositoriesExpressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory>builder()
                .put(SysRepositoriesTableInfo.Columns.NAME, new RowCollectExpressionFactory() {
                    @Override
                    public RowCollectExpression create() {
                        return new RowContextCollectorExpression<SysRepository, BytesRef>() {
                            @Override
                            public BytesRef value() {
                                return new BytesRef(row.name());
                            }
                        };
                    }
                })
                .put(SysRepositoriesTableInfo.Columns.TYPE, new RowCollectExpressionFactory() {
                    @Override
                    public RowCollectExpression create() {
                        return new RowContextCollectorExpression<SysRepository, BytesRef>() {
                            @Override
                            public BytesRef value() {
                                return new BytesRef(row.type());
                            }
                        };
                    }
                })
                .put(SysRepositoriesTableInfo.Columns.SETTINGS, new RowCollectExpressionFactory() {
                    @Override
                    public RowCollectExpression create() {
                        return new RowContextCollectorExpression<SysRepository, Map<String, Object>>() {
                            @Override
                            public Map<String, Object> value() {
                                return row.settings();
                            }
                        };
                    }
                })
                .build();
    }

    private ImmutableMap<ColumnIdent, RowCollectExpressionFactory> getSysSnapshotsExpressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory>builder()
                .put(SysSnapshotsTableInfo.Columns.NAME, new RowCollectExpressionFactory() {
                    @Override
                    public RowCollectExpression create() {
                        return new RowContextCollectorExpression<SysSnapshot, BytesRef>() {
                            @Override
                            public BytesRef value() {
                                return new BytesRef(row.name());
                            }
                        };
                    }
                })
                .put(SysSnapshotsTableInfo.Columns.REPOSITORY, new RowCollectExpressionFactory() {
                    @Override
                    public RowCollectExpression create() {
                        return new RowContextCollectorExpression<SysSnapshot, BytesRef>() {
                            @Override
                            public BytesRef value() {
                                return new BytesRef(row.repository());
                            }
                        };
                    }
                })
                .put(SysSnapshotsTableInfo.Columns.CONCRETE_INDICES, new RowCollectExpressionFactory() {
                    @Override
                    public RowCollectExpression create() {
                        return new RowContextCollectorExpression<SysSnapshot, BytesRef[]>() {
                            @Override
                            public BytesRef[] value() {
                                return Lists.transform(row.concreteIndices(), new Function<String, BytesRef>() {
                                    @Nullable
                                    @Override
                                    public BytesRef apply(String input) {
                                        if (input == null) {
                                            return null;
                                        }
                                        return new BytesRef(input);
                                    }
                                }).toArray(new BytesRef[row.concreteIndices().size()]);
                            }
                        };
                    }
                })
                .put(SysSnapshotsTableInfo.Columns.STARTED, new RowCollectExpressionFactory() {
                    @Override
                    public RowCollectExpression create() {
                        return new RowContextCollectorExpression<SysSnapshot, Long>() {
                            @Override
                            public Long value() {
                                return row.started();
                            }
                        };
                    }
                })
                .put(SysSnapshotsTableInfo.Columns.FINISHED, new RowCollectExpressionFactory() {
                    @Override
                    public RowCollectExpression create() {
                        return new RowContextCollectorExpression<SysSnapshot, Long>() {
                            @Override
                            public Long value() {
                                return row.finished();
                            }
                        };
                    }
                })
                .put(SysSnapshotsTableInfo.Columns.VERSION, new RowCollectExpressionFactory() {
                    @Override
                    public RowCollectExpression create() {
                        return new RowContextCollectorExpression<SysSnapshot, BytesRef>() {
                            @Override
                            public BytesRef value() {
                                return new BytesRef(row.version());
                            }
                        };
                    }
                })
                .put(SysSnapshotsTableInfo.Columns.STATE, new RowCollectExpressionFactory() {
                    @Override
                    public RowCollectExpression create() {
                        return new RowContextCollectorExpression<SysSnapshot, BytesRef>() {
                            @Override
                            public BytesRef value() {
                                return new BytesRef(row.state());
                            }
                        };
                    }
                })
                .build();
    }

    @Override
    public RowCollectExpression<?, ?> getImplementation(Reference refInfo) {
        return rowCollectExpressionFromFactoryMap(tableFactories, refInfo);
    }

    public static RowCollectExpression<?, ?> rowCollectExpressionFromFactoryMap(
            Map<TableIdent, Map<ColumnIdent, RowCollectExpressionFactory>> factoryMap,
            Reference info) {

        Map<ColumnIdent, RowCollectExpressionFactory> innerFactories = factoryMap.get(info.ident().tableIdent());
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
            Map<ColumnIdent, RowCollectExpressionFactory> innerFactories,
            ColumnIdent columnIdent) {

        RowCollectExpressionFactory factory = innerFactories.get(columnIdent.getRoot());
        if (factory == null) {
            return null;
        }
        ReferenceImplementation referenceImplementation = factory.create();
        for (String part : columnIdent.path()) {
            referenceImplementation = referenceImplementation.getChildImplementation(part);
            if (referenceImplementation == null) {
                return null;
            }
        }
        return (RowCollectExpression<?, ?>) referenceImplementation;
    }
}
