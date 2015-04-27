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
import io.crate.metadata.sys.SysOperationsTableInfo;
import org.apache.lucene.util.BytesRef;

public abstract class SysOperationExpression<T> extends RowContextCollectorExpression<OperationContext, T> {

    public static final ImmutableList<SysOperationExpression<?>> IMPLEMENTATIONS =
            ImmutableList.<SysOperationExpression<?>>builder()
            .add(new SysOperationExpression<BytesRef>(SysOperationsTableInfo.ColumnNames.ID) {
                @Override
                public BytesRef value() {
                    return new BytesRef(Integer.toString(row.id));
                }
            })
            .add(new SysOperationExpression<BytesRef>(SysOperationsTableInfo.ColumnNames.JOB_ID) {
                @Override
                public BytesRef value() {
                    return new BytesRef(row.jobId.toString());
                }
            })
            .add(new SysOperationExpression<BytesRef>(SysOperationsTableInfo.ColumnNames.NAME) {
                @Override
                public BytesRef value() {
                    return new BytesRef(row.name);
                }
            })
            .add(new SysOperationExpression<Long>(SysOperationsTableInfo.ColumnNames.STARTED) {
                @Override
                public Long value() {
                    return row.started;
                }
            })
            .add(new SysOperationExpression<Long>(SysOperationsTableInfo.ColumnNames.USED_BYTES) {
                @Override
                public Long value() {
                    if (row.usedBytes == 0) {
                        return null;
                    }
                    return row.usedBytes;
                }
            }).build();

    public SysOperationExpression(String name) {
        super(SysOperationsTableInfo.columnInfo(new ColumnIdent(name)));
    }
}