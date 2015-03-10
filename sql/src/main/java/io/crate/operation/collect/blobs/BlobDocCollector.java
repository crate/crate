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

package io.crate.operation.collect.blobs;

import io.crate.blob.BlobContainer;
import io.crate.blob.v2.BlobShard;
import io.crate.breaker.RamAccountingContext;
import io.crate.operation.Input;
import io.crate.operation.InputRow;
import io.crate.operation.RowDownstream;
import io.crate.operation.RowDownstreamHandle;
import io.crate.operation.collect.CrateCollector;

import java.io.File;
import java.util.List;

public class BlobDocCollector implements CrateCollector {

    private final BlobShard blobShard;
    private final List<Input<?>> inputs;
    private final List<BlobCollectorExpression<?>> expressions;
    private final Input<Boolean> condition;

    private RowDownstreamHandle downstream;

    public BlobDocCollector(
            BlobShard blobShard,
            List<Input<?>> inputs,
            List<BlobCollectorExpression<?>> expressions,
            Input<Boolean> condition,
            RowDownstream downstream) {
        this.blobShard = blobShard;
        this.inputs = inputs;
        this.expressions = expressions;
        this.condition = condition;
        this.downstream = downstream.registerUpstream(this);
    }

    @Override
    public void doCollect(RamAccountingContext ramAccountingContext) throws Exception {
        BlobContainer.FileVisitor fileVisitor = new FileListingsFileVisitor();
        try {
            blobShard.blobContainer().walkFiles(null, fileVisitor);
        } finally {
            downstream.finish();
        }
    }

    private class FileListingsFileVisitor implements BlobContainer.FileVisitor {

        private final InputRow row = new InputRow(inputs);

        @Override
        public boolean visit(File file) {
            for (BlobCollectorExpression expression : expressions) {
                expression.setNextBlob(file);
            }
            if (condition.value()) {
                return downstream.setNextRow(row);
            }
            return true;
        }
    }
}
