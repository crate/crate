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
import io.crate.operation.Input;
import io.crate.operation.InputRow;
import io.crate.operation.RowUpstream;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.collect.CrateCollector;
import io.crate.operation.projectors.RowFilter;
import io.crate.operation.projectors.RowReceiver;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CancellationException;

public class BlobDocCollector implements CrateCollector, RowUpstream {

    private final RowFilter<File> rowFilter;
    private BlobContainer blobContainer;
    private final List<Input<?>> inputs;

    private RowReceiver downstream;
    private volatile boolean killed;

    public BlobDocCollector(
            BlobContainer blobContainer,
            List<Input<?>> inputs,
            List<CollectExpression<File, ?>> expressions,
            Input<Boolean> condition,
            RowReceiver downstream) {
        this.blobContainer = blobContainer;
        this.inputs = inputs;
        this.rowFilter = new RowFilter<>(expressions, condition);
        this.downstream = downstream;
        downstream.setUpstream(this);
    }

    @Override
    public void doCollect() {
        BlobContainer.FileVisitor fileVisitor = new FileListingsFileVisitor();
        try {
            blobContainer.walkFiles(null, fileVisitor);
            downstream.finish();
        } catch (Throwable t) {
            downstream.fail(t);
        }
    }

    @Override
    public void kill(@Nullable Throwable throwable) {
        killed = true;
    }

    @Override
    public void pause() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void resume(boolean async) {
        throw new UnsupportedOperationException();
    }

    /**
     * tells the RowUpstream that it should push all rows again
     */
    @Override
    public void repeat() {
        throw new UnsupportedOperationException();
    }

    private class FileListingsFileVisitor implements BlobContainer.FileVisitor {

        private final InputRow row = new InputRow(inputs);

        @Override
        public boolean visit(File file) throws IOException {
            if (killed) {
                throw new CancellationException();
            }
            //noinspection SimplifiableIfStatement
            if (rowFilter.matches(file)) {
                return downstream.setNextRow(row);
            }
            return true;
        }
    }
}
