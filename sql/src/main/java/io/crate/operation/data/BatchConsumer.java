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

import java.util.List;
import java.util.function.Consumer;

/**
 * This interface represents a downstream for data. Upstream components can emit data by calling
 * {@link #accept} with an implementation of {@link BatchCursor}, which then in turn is used by the downstream component
 * to consume the data when needed.
 *
 * If the upstream fails to create the cursor for some reason, it is required to call {@link #fail} in order to notify
 * the downstream. The upstream must only call either {@link #accept} or {@link #fail} and never both.
 */
public interface BatchConsumer extends Consumer<BatchCursor> {


    /**
     * Prepends projectors to the consumer.
     *
     * @param projectors a List of {@link BatchProjector} instances to be prepended
     * @return a new consumer which calls the original consumer with the projected cursor when called
     */
    default BatchConsumer projected(List<BatchProjector> projectors){
        return new ProjectedBatchConsumer(projectors, this);
    }


    /**
     * Must be called in case of a failure which prevents the upstream from creating a cursor in order
     * to call {@link #accept}
     * @param throwable the cause of the failure
     */
    default void fail(Throwable throwable){

    }

    /**
     * Accepts the given cursor and does some operation on it. When this method is called, the given cursor is
     * required to be ready for use. It might also be the case that the consumer consumes the whole cursors synchronously.
     *
     * The given cursor must be kept valid until {@link BatchCursor#close()} is called by the consumer, therefore
     * implementations of this interface are required to ensure that {@link BatchCursor#close()} is called on
     * the cursor eventually.
     *
     * @param batchCursor the cursor to be consumed
     */
    @Override
    void accept(BatchCursor batchCursor);

    /**
     * @return true if the consumer wants to scroll backwards by using {@link BatchCursor#moveFirst()}
     */
    default boolean requiresScroll(){
        return false;
    }
}
