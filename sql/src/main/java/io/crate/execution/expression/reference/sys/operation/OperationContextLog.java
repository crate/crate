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

package io.crate.execution.expression.reference.sys.operation;

import io.crate.execution.expression.reference.sys.job.ContextLog;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.UUID;

public class OperationContextLog implements ContextLog {

    private final OperationContext operationContext;
    @Nullable
    private final String errorMessage;
    private final long ended;

    public OperationContextLog(OperationContext operationContext, @Nullable String errorMessage) {
        this.operationContext = operationContext;
        this.errorMessage = errorMessage;
        this.ended = System.currentTimeMillis();
    }

    public int id() {
        return operationContext.id;
    }

    public UUID jobId() {
        return operationContext.jobId;
    }

    public String name() {
        return operationContext.name;
    }

    public long started() {
        return operationContext.started;
    }

    @Override
    public long ended() {
        return ended;
    }

    public long usedBytes() {
        return operationContext.usedBytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OperationContextLog that = (OperationContextLog) o;
        return operationContext.equals(that.operationContext);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operationContext);
    }

    @Nullable
    public String errorMessage() {
        return errorMessage;
    }

}
