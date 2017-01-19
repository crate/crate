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

package io.crate.action.sql;

import com.google.common.util.concurrent.MoreExecutors;
import io.crate.operation.data.BatchConsumer;
import io.crate.operation.data.BatchCursor;

public class RowReceiverToResultReceiver implements BatchConsumer {

    private ResultReceiver resultReceiver;
    private int maxRows;
    private long rowCount = 0;
    private BatchCursor cursor;

    public RowReceiverToResultReceiver(ResultReceiver resultReceiver, int maxRows) {
        this.resultReceiver = resultReceiver;
        this.maxRows = maxRows;
    }

    private void consume() {
        System.err.println("RR2RR consume");
        if (cursor.status() == BatchCursor.Status.ON_ROW) {
            do {
                rowCount++;
                System.err.println("RR2RR consume in row=" + rowCount);
                try {
                    resultReceiver.setNextRow(this.cursor);
                } catch (Throwable t){
                    // since the ResultReceiver is still row based, we need to watch if the row fails and propagate
                    // the failure to the downstream and close the cursor. such a failure might occur if a scalar
                    // throws an exception, e.g: division by zero
                    resultReceiver.fail(t);
                    System.err.println("RR2RR setNextRow failed " + t);
                    cursor.close();
                    return;
                }
                if (maxRows > 0 && rowCount == maxRows) {
                    resultReceiver.batchFinished();
                    return;
                }
            } while (cursor.moveNext());
        }
        if (!cursor.allLoaded()) {
            cursor.loadNextBatch().addListener(this::consume, MoreExecutors.directExecutor());
        } else {
            System.err.println("RR2RR consume finished");
            resultReceiver.allFinished();
            cursor.close();
        }
    }

    /*
    resumes after the batch has finished
     */
    public void resume() {
        cursor.moveNext();
        consume();
    }

    @Override
    public void accept(BatchCursor batchCursor,  Throwable t) {
        if (batchCursor != null){
            this.cursor = batchCursor;
            consume();
        } else {
            resultReceiver.fail(t);
        }
    }

    public void replaceResultReceiver(ResultReceiver resultReceiver, int maxRows) {
        // XDOBE: check if setting rowcount to 0 is ok
        this.rowCount = 0;
        this.resultReceiver = resultReceiver;
        this.maxRows = maxRows;
    }

    /*
    closes the underlying upstream cursor if not already closed. returns true if the cursor was closed
     */
    public boolean close() {
        if (cursor != null && cursor.status() != BatchCursor.Status.CLOSED) {
            cursor.close();
            return true;
        }
        return false;
    }
}
