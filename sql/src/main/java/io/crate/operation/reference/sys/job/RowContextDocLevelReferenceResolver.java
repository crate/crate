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

package io.crate.operation.reference.sys.job;

import com.google.common.collect.ImmutableMap;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceImplementation;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.RowCollectExpression;
import io.crate.metadata.RowContextCollectorExpression;
import io.crate.metadata.TableIdent;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.sys.SysChecksTableInfo;
import io.crate.metadata.sys.SysJobsLogTableInfo;
import io.crate.metadata.sys.SysJobsTableInfo;
import io.crate.metadata.sys.SysOperationsLogTableInfo;
import io.crate.metadata.sys.SysOperationsTableInfo;
import io.crate.operation.reference.DocLevelReferenceResolver;
import io.crate.operation.reference.sys.check.checks.SysCheck;
import io.crate.operation.reference.sys.operation.OperationContext;
import io.crate.operation.reference.sys.operation.OperationContextLog;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.lucene.BytesRefs;

import java.util.HashMap;
import java.util.Map;

@Singleton
public class RowContextDocLevelReferenceResolver implements DocLevelReferenceResolver<RowCollectExpression<?, ?>> {

    public static final RowContextDocLevelReferenceResolver INSTANCE = new RowContextDocLevelReferenceResolver();

    private final Map<TableIdent, Map<ColumnIdent, RowCollectExpressionFactory>> tableFactories = new HashMap<>();

    /**
     * This is a singleton Use the static INSTANCE attribute
     */
    private RowContextDocLevelReferenceResolver() {
        tableFactories.put(SysJobsTableInfo.IDENT, getSysJobsExpressions());
        tableFactories.put(SysJobsLogTableInfo.IDENT, getSysJobsLogExpressions());
        tableFactories.put(SysOperationsTableInfo.IDENT, getSysOperationExpressions());
        tableFactories.put(SysOperationsLogTableInfo.IDENT, getSysOperationLogExpressions());
        tableFactories.put(SysChecksTableInfo.IDENT, getSysChecksExpressions());
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

    @Override
    public RowCollectExpression<?, ?> getImplementation(ReferenceInfo info) {
        return rowCollectExpressionFromFactoryMap(tableFactories, info);
    }

    public static RowCollectExpression<?, ?> rowCollectExpressionFromFactoryMap(
            Map<TableIdent, Map<ColumnIdent, RowCollectExpressionFactory>> factoryMap,
            ReferenceInfo info) {

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
