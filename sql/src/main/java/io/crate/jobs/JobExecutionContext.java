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

package io.crate.jobs;

import io.crate.operation.collect.JobCollectContext;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.lease.Releasable;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public class JobExecutionContext implements Releasable {

    private final UUID jobId;
    private final JobCollectContext collectContext;
    private final long keepAlive;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private volatile long lastAccessTime = -1;


    public JobExecutionContext(UUID jobId, long keepAlive) {
        this.jobId = jobId;
        this.keepAlive = keepAlive;
        this.collectContext = new JobCollectContext(jobId);
    }

    public JobCollectContext collectContext() {
        return collectContext;
    }

    public void accessed(long accessTime) {
        this.lastAccessTime = accessTime;
    }

    public long lastAccessTime() {
        return this.lastAccessTime;
    }

    public long keepAlive() {
        return this.keepAlive;
    }

    @Override
    public void close() throws ElasticsearchException {
        if (closed.compareAndSet(false, true)) { // prevent double release
            collectContext.close();
        }
    }

    public UUID id() {
        return jobId;
    }
}
