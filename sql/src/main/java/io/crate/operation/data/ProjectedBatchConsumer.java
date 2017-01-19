/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.operation.data;

import com.google.common.base.Preconditions;

import java.util.List;

/**
 * A consumer class which applies projectors to cursors before handing it to the downstream consumer.
 *
 * The implementation inherits its scrolling requirement from the downstream, however if any of the applied projectors
 * has a scrollable result it does not require scrolling.
 */
class ProjectedBatchConsumer implements BatchConsumer {

    private final boolean requiresScroll;
    private final List<? extends BatchProjector> rowReceivers;
    private final BatchConsumer downstream;

    ProjectedBatchConsumer(List<? extends BatchProjector> projectors, BatchConsumer downstream) {
        Preconditions.checkArgument(!projectors.isEmpty(), "no projectors given");
        this.rowReceivers = projectors;
        this.downstream = downstream;
        if (downstream.requiresScroll()) {
            boolean isScrollable = false;
            for (BatchProjector projector : projectors) {
                if (projector.isScrollable()) {
                    isScrollable = true;
                    break;
                }
            }
            requiresScroll = !isScrollable;
        } else {
            requiresScroll = false;
        }
    }

    @Override
    public void accept(BatchCursor batchCursor, Throwable t) {
        if (batchCursor != null) {
            try {
                for (BatchProjector rowReceiver : rowReceivers) {
                    batchCursor = rowReceiver.apply(batchCursor);
                }
            } catch (Throwable throwable){
                t = throwable;
            }
        }
        downstream.accept(batchCursor, t);
    }

    @Override
    public boolean requiresScroll() {
        return requiresScroll;
    }
}
