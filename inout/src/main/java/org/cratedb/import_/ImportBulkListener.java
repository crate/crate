/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package org.cratedb.import_;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.common.util.concurrent.BaseFuture;

import org.cratedb.import_.Importer.ImportCounts;

public class ImportBulkListener extends BaseFuture<ImportBulkListener> implements BulkProcessor.Listener {

    private AtomicLong bulksInProgress = new AtomicLong();
    private ImportCounts counts = new ImportCounts();

    public ImportBulkListener(String fileName) {
        counts.fileName = fileName;
    }

    @Override
    public ImportBulkListener get() throws InterruptedException,
            ExecutionException {
        if (bulksInProgress.get() == 0) {
            return this;
        }
        return super.get();
    }

    public void addFailure() {
        counts.failures++;
    }

    public ImportCounts importCounts() {
        return counts;
    }

    @Override
    public void beforeBulk(long executionId, BulkRequest request) {
        bulksInProgress.incrementAndGet();
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request,
            BulkResponse response) {
        bulksInProgress.decrementAndGet();
        if (response.hasFailures()) {
            for (BulkItemResponse item : response.getItems()) {
                if (item.isFailed()) {
                    counts.failures++;
                } else {
                    counts.successes++;
                }
            }
        } else {
            counts.successes += response.getItems().length;
        }
        checkRelease();
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request,
            Throwable failure) {
        bulksInProgress.decrementAndGet();
        counts.failures += request.requests().size();
        failure.printStackTrace();
        checkRelease();
    }

    private void checkRelease() {
        if (bulksInProgress.get() == 0) {
            this.set(this);
        }
    }

    public void addInvalid() {
        counts.invalid++;
    }

}