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

import com.google.common.collect.ImmutableList;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RowContextCollectorExpression;
import io.crate.metadata.sys.SysJobsLogTableInfo;
import org.apache.lucene.util.BytesRef;

public abstract class SysJobLogExpression<T> extends RowContextCollectorExpression<JobContextLog, T> {

    static final String ID = "id";
    static final String STMT = "stmt";
    static final String STARTED = "started";
    static final String ENDED = "ended";
    static final String ERROR = "error";

    public static final ImmutableList<SysJobLogExpression<?>> IMPLEMENTATIONS =
            ImmutableList.<SysJobLogExpression<?>>builder()
            .add(new SysJobLogExpression<BytesRef>(ID) {
                @Override
                public BytesRef value() {
                    return new BytesRef(row.id().toString());
                }
            })
            .add(new SysJobLogExpression<BytesRef>(STMT) {
                @Override
                public BytesRef value() {
                    return new BytesRef(row.statement());
                }
            })
            .add(new SysJobLogExpression<Long>(STARTED) {
                @Override
                public Long value() {
                    return row.started();
                }
            })
            .add(new SysJobLogExpression<Long>(ENDED) {
                @Override
                public Long value() {
                    return row.ended();
                }
            })
            .add(new SysJobLogExpression<BytesRef>(ERROR) {
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

    protected SysJobLogExpression(String name) {
        super(SysJobsLogTableInfo.INFOS.get(new ColumnIdent(name)));
    }
}
