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
import io.crate.operation.Input;
import io.crate.operation.collect.CrateCollector;
import io.crate.operation.projectors.Projector;

import java.io.File;
import java.util.List;

public class BlobDocCollector implements CrateCollector {

    private final BlobShard blobShard;
    private final List<Input<?>> inputs;
    private final List<BlobCollectorExpression<?>> expressions;
    private final Input<Boolean> condition;

    private Projector downstream;

    public BlobDocCollector(
            BlobShard blobShard,
            List<Input<?>> inputs,
            List<BlobCollectorExpression<?>> expressions,
            Input<Boolean> condition,
            Projector downstream) {
        this.blobShard = blobShard;
        this.inputs = inputs;
        this.expressions = expressions;
        this.condition = condition;
        downstream(downstream);
    }

    @Override
    public void doCollect() throws Exception {
        BlobContainer.FileVisitor fileVisitor = new FileListingsFileVisitor();
        try {
            blobShard.blobContainer().walkFiles(null, fileVisitor);
        } finally {
            downstream.upstreamFinished();
        }
    }

    private class FileListingsFileVisitor implements BlobContainer.FileVisitor {

        @Override
        public boolean visit(File file) {
            for (BlobCollectorExpression expression : expressions) {
                expression.setNextBlob(file);
            }
            if (condition.value()) {
                Object[] newRow = new Object[inputs.size()];

                int i = 0;
                for (Input<?> input : inputs) {
                    newRow[i++] = input.value();
                }

                return downstream.setNextRow(newRow);
            }
            return true;
        }
    }

    @Override
    public void downstream(Projector downstream) {
        this.downstream = downstream;
        downstream.registerUpstream(this);
    }

    @Override
    public Projector downstream() {
        return this.downstream;
    }

}
