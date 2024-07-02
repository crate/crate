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

package io.crate.expression.reference.sys.job;

import org.jetbrains.annotations.VisibleForTesting;
import io.crate.planner.operators.StatementClassifier;

import org.jetbrains.annotations.Nullable;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;

import java.util.UUID;

public class JobContextLog implements ContextLog, Accountable {

    private final JobContext jobContext;

    @Nullable
    private final String errorMessage;

    private final long ended;

    public JobContextLog(JobContext jobContext, @Nullable String errorMessage) {
        this.jobContext = jobContext;
        this.errorMessage = errorMessage;
        this.ended = System.currentTimeMillis();
    }

    @VisibleForTesting
    public JobContextLog(JobContext jobContext, @Nullable String errorMessage, long ended) {
        this.jobContext = jobContext;
        this.errorMessage = errorMessage;
        this.ended = ended;
    }

    public UUID id() {
        return jobContext.id();
    }

    @Nullable
    public String username() {
        return jobContext.username();
    }

    public String statement() {
        return jobContext.stmt();
    }

    public long started() {
        return jobContext.started();
    }

    @Nullable
    public StatementClassifier.Classification classification() {
        return jobContext.classification();
    }

    @Override
    public long ended() {
        return ended;
    }

    @Nullable
    public String errorMessage() {
        return errorMessage;
    }

    @Override
    public long ramBytesUsed() {
        long size = 0L;

        // JobContextLog
        size += 32L; // 24 bytes (ref+headers) + 8 bytes (ended)
        size += errorMessage == null ? 0 : errorMessage.length();

        // JobContext
        size += 52L; // 24 bytes (ref+headers) + 4 bytes (id) + 8 bytes (started) + 16 bytes (uuid)
        size += jobContext.stmt().length();

        return RamUsageEstimator.alignObjectSize(size);
    }
}
