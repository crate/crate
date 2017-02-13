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

import java.util.concurrent.CompletableFuture;


/**
 * A batch cursor used to navigate over batched results. Users of this interface must not rely on any thread safety of
 * the underlying implementation.
 */
public interface BatchCursor extends Row {

    /**
     * Moves the cursor to the first row if there is one.
     *
     * @return true if the cursor is on a valid row; false if there are no rows.
     */
    boolean moveFirst();

    /**
     * Advances the cursor to the next row if there is one.
     *
     * @return true if the cursor is on a valid row; false if there was no next row.
     */
    boolean moveNext();

    /**
     * Closes the cursor and frees all resources.
     */
    void close();

    /**
     * loads the next batch if there is still data available for loading, see {@link #allLoaded()}.
     * <p>
     * NOTE: while loading takes place the cursor must not be moved.
     *
     * @return a future which will be completed once the loading is done.
     */
    CompletableFuture<Status> loadNextBatch();

    /**
     * @return true if all underlying data is already loaded
     */
    boolean allLoaded();

    boolean onRow();

    enum Status {
        OFF_ROW,
        ON_ROW
    }
}
