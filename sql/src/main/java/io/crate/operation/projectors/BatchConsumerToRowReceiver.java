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

package io.crate.operation.projectors;

import io.crate.data.BatchConsumer;
import io.crate.data.BatchCursor;

import java.util.Objects;

public class BatchConsumerToRowReceiver implements BatchConsumer {

    private final RowReceiver rowReceiver;

    public BatchConsumerToRowReceiver(RowReceiver rowReceiver) {
        Objects.requireNonNull(rowReceiver, "RowReceiver cannot be null");
        this.rowReceiver = rowReceiver;
    }

    @Override
    public void accept(BatchCursor cursor, Throwable failure) {
        if (failure == null) {
            consumeCursor(cursor);
        } else {
            rowReceiver.fail(failure);
        }
    }

    private void consumeCursor(BatchCursor cursor) {
        while (cursor.moveNext()) {
            RowReceiver.Result result = rowReceiver.setNextRow(cursor);
            switch (result) {
                case CONTINUE:
                    break;
                case STOP:
                    rowReceiver.finish(RepeatHandle.UNSUPPORTED);
                    cursor.close();
                    return;
                case PAUSE:
                    rowReceiver.pauseProcessed(async -> consumeCursor(cursor));
                    return;
            }
        }

        if (cursor.allLoaded()) {
            cursor.close();
            rowReceiver.finish(RepeatHandle.UNSUPPORTED);
        } else {
            cursor.loadNextBatch().whenComplete(
                (r, e) -> {
                    if (e != null) {
                        rowReceiver.fail(e);
                        cursor.close();
                    } else {
                        consumeCursor(cursor);
                    }
                }
            );
        }
    }
}
