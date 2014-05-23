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
import io.crate.metadata.InMemoryCollectorExpression;
import io.crate.metadata.sys.SysJobsTableInfo;
import org.apache.lucene.util.BytesRef;

public abstract class SysJobExpression<T>
        extends InMemoryCollectorExpression<JobContext, T> {

    private static final String ID = "id";
    private static final String STMT = "stmt";
    private static final String STARTED = "started";

    public static final ImmutableList<SysJobExpression<?>> IMPLEMENTATIONS
            = ImmutableList.<SysJobExpression<?>>builder()
            .add(new SysJobExpression<BytesRef>(ID) {
                @Override
                public BytesRef value() {
                    return new BytesRef(row.id);
                }
            })
            .add(new SysJobExpression<BytesRef>(STMT) {
                @Override
                public BytesRef value() {
                    return new BytesRef(row.stmt);
                }
            })
            .add(new SysJobExpression<Long>(STARTED) {
                @Override
                public Long value() {
                    return row.started;
                }
            })
            .build();


    protected SysJobExpression(String name) {
        super(SysJobsTableInfo.INFOS.get(new ColumnIdent(name)));
    }
}
