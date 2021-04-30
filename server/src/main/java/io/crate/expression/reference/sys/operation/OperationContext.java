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

import java.util.Objects;
import java.util.UUID;
import java.util.function.LongSupplier;

public class OperationContext {

    public final int id;
    public final UUID jobId;
    public final String name;

    public final long started;
    private final LongSupplier bytesUsed;

    public OperationContext(int id, UUID jobId, String name, long started, LongSupplier bytesUsed) {
        this.id = id;
        this.jobId = jobId;
        this.name = name;
        this.started = started;
        this.bytesUsed = bytesUsed;
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

    public long usedBytes() {
        return bytesUsed.getAsLong();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OperationContext that = (OperationContext) o;
        if (id != that.id) return false;
        return jobId.equals(that.jobId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, jobId);
    }
}
