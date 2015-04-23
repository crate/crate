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

package io.crate.operation.reference.sys.operation;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RowContextCollectorExpression;
import io.crate.metadata.sys.SysOperationsLogTableInfo;
import org.apache.lucene.util.BytesRef;

public abstract class SysOperationLogExpression<T> extends RowContextCollectorExpression<OperationContextLog, T> {

    public static final ImmutableList<SysOperationLogExpression<?>> IMPLEMENTATIONS =
            ImmutableList.<SysOperationLogExpression<?>>builder()
            .add(new SysOperationLogExpression<BytesRef>(SysOperationsLogTableInfo.ColumnNames.ID) {
                @Override
                public BytesRef value() {
                    return new BytesRef(Integer.toString(row.id()));
                }
            })
            .add(new SysOperationLogExpression<BytesRef>(SysOperationsLogTableInfo.ColumnNames.JOB_ID) {
                @Override
                public BytesRef value() {
                    return new BytesRef(row.jobId().toString());
                }
            })
            .add(new SysOperationLogExpression<BytesRef>(SysOperationsLogTableInfo.ColumnNames.NAME) {
                @Override
                public BytesRef value() {
                    return new BytesRef(row.name());
                }
            })
            .add(new SysOperationLogExpression<Long>(SysOperationsLogTableInfo.ColumnNames.STARTED) {
                @Override
                public Long value() {
                    return row.started();
                }
            })
            .add(new SysOperationLogExpression<Long>(SysOperationsLogTableInfo.ColumnNames.ENDED) {
                @Override
                public Long value() {
                    return row.ended();
                }
            })
            .add(new SysOperationLogExpression<Long>(SysOperationsLogTableInfo.ColumnNames.USED_BYTES) {
                @Override
                public Long value() {
                    if (row.usedBytes() == 0) {
                        return null;
                    }
                    return row.usedBytes();
                }
            })
            .add(new SysOperationLogExpression<BytesRef>(SysOperationsLogTableInfo.ColumnNames.ERROR) {
                @Override
                public BytesRef value() {
                    String errorMessage = row.errorMessage();
                    if (errorMessage == null) {
                        return null;
                    }
                    return new BytesRef(errorMessage);
                }
            }).build();

    public SysOperationLogExpression(String name) {
        super(SysOperationsLogTableInfo.columnInfo(new ColumnIdent(name)));
    }
}
