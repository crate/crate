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

package io.crate.data;

import java.util.List;
import java.util.function.BiConsumer;

/**
 * This interface represents a consumer for data, where upstreams use the {@link #accept} method to emit data.
 * Upstreams are responsible to always call {@link #accept}, both on success or on failure to ensure that the
 * consumer can react on upstream failures.
 *
 * A consumer can only be called once, which also means that only one upstream must have the responsibility for emitting
 * to a downstream.
 *
 *
 *
 *
 *
 */

/*
@startuml

title Basic successful consume sequence

participant Upstream as u
participant Cursor as cu
participant Consumer as c

u -> c:requiresScroll ?
create cu
u -> cu: new
u -> c:accept(cursor, null)

note over c: consume cursor

c -> cu:status() == ON_ROW?

loop as long as moveNext()==true
c -> cu: use Row interface for data access
c -> cu:moveNext()
end

c -> cu:allLoaded ?

alt not allLoaded?
c -> cu: future = loadNextBatch()
...when future is done...
note over c: goto consume cursor
end

c -> cu: close()
cu -> u: cleanup();

@enduml
 */

public interface BatchConsumer extends BiConsumer<BatchCursor, Throwable> {

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
     * Accepts the given cursor and performs some operation on it. When this method is called, the given cursor is
     * required to be ready for use. It might also be the case that the consumer consumes the whole cursors synchronously.
     * The given cursor must be kept valid until {@link BatchCursor#close()} is called by the consumer, therefore
     * implementations of this interface are required to ensure that {@link BatchCursor#close()} is called on
     * the cursor eventually.
     *
     * In case of success the cursor must not be null and the failure must be defined. In case of a failure to create
     * a cursor the cursor argument must be set to null and the failure not null.
     *
     * @param cursor the cursor to be consumed or null if a failure occured
     * @param failure the cause of the failure or null if successful
     *
     */
    @Override
    void accept(BatchCursor cursor, Throwable failure);

    /**
     * @return true if the consumer wants to scroll backwards by using {@link BatchCursor#moveFirst()}
     */
    default boolean requiresScroll(){
        return false;
    }

}
