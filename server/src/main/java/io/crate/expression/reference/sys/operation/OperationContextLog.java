/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.expression.reference.sys.operation;

import io.crate.expression.reference.sys.job.ContextLog;

import javax.annotation.Nullable;
import java.util.UUID;

public class OperationContextLog implements ContextLog {

    @Nullable
    private final String errorMessage;
    private final long ended;
    private final int id;
    private final UUID jobId;
    private final String name;
    private final long started;
    private final long usedBytes;

    public OperationContextLog(OperationContext operationContext, @Nullable String errorMessage) {
        // We don't want to have a reference to operationContext so that it can be GC'd
        this.id = operationContext.id();
        this.jobId = operationContext.jobId();
        this.name = operationContext.name();
        this.started = operationContext.started();
        this.usedBytes = operationContext.usedBytes();
        this.errorMessage = errorMessage;
        this.ended = System.currentTimeMillis();
    }

    public int id() {
        return id;
    }

    public UUID jobId() {
        return jobId;
    }

    public String name() {
        return name;
    }

    public long started() {
        return started;
    }

    @Override
    public long ended() {
        return ended;
    }

    public long usedBytes() {
        return usedBytes;
    }

    @Nullable
    public String errorMessage() {
        return errorMessage;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        OperationContextLog that = (OperationContextLog) o;

        if (id != that.id) {
            return false;
        }
        return jobId.equals(that.jobId);
    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + jobId.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "OperationContextLog{" +
               "errorMessage='" + errorMessage + '\'' +
               ", id=" + id +
               ", jobId=" + jobId +
               ", name='" + name + '\'' +
               ", usedBytes=" + usedBytes +
               '}';
    }
}
