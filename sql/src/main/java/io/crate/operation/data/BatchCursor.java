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

import com.google.common.util.concurrent.ListenableFuture;
import io.crate.core.collections.Row;


/**
 * A batch cursor used to navigate over batched results. Users of this interface must not rely on any thread safety of
 * the underlying implementation.
 */
public interface BatchCursor extends Row {

    enum Status {
        ON_ROW,  // on a valid row, ready to get data from
        OFF_ROW, // at the end of a batch or no rows available
        LOADING, // loading data internally
        CLOSED // the cursor is closed and therefore not usable
    }

    /**
     * Moves the cursor to the first row if there is one.
     *
     * @return true if the cursor is on a valid row; false if there are no rows.
     */
    boolean moveFirst();

    /**
     * Advances the cursor to the next row if there is one. Calling {@link  #status()} will return {@link Status.ON_ROW}
     * if positioned successfully, or {@link Status.OFF_ROW} if no more rows are available.
     *
     * @return true if the cursor is on a valid row; false if there was no next row.
     */
    boolean moveNext();

    /**
     * Closes the cursor and frees all resources. The status is changing to {@link Status.CLOSED} and the cursors
     * is not usable anymore.
     */
    void close();

    /**
     * @return the current status of this cursor. see {@link Status}
     */
    Status status();

    /**
     * loads the next batch, this method is only allowed to be called if the status is {@link Status.OFF_ROW} and
     * there is still data available for loading, see {@link #allLoaded()}.
     *
     * NOTE: while loading takes place the cursor must not be moved. {@link #status()} will return LOADING
     * to indicate this.
     *
     * @return a future which will be completed once the loading is done.
     */
    ListenableFuture<?> loadNextBatch();

    /**
     * @return true if all underlying data is already loaded
     */
    boolean allLoaded();


}
