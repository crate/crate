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

package io.crate.execution;

import io.crate.core.collections.Row;
import io.crate.operation.projectors.RepeatHandle;
import io.crate.operation.projectors.RowReceiver;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

public class Driver {

    private final DataSource source;
    private final RowReceiver receiver;


    public Driver(DataSource source, RowReceiver receiver) {
        this.source = source;
        this.receiver = receiver;
    }

    private void consumeIt(Iterator<Row> it, boolean isLast) {
        while (it.hasNext()) {
            Row row = it.next();
            RowReceiver.Result result = receiver.setNextRow(row);
            switch (result) {
                case CONTINUE:
                    continue;
                case STOP:
                    receiver.finish(RepeatHandle.UNSUPPORTED);
                    source.close();
                    return;
                case PAUSE:
                    receiver.pauseProcessed(async -> consumeIt(it, isLast));
                    return;
            }
        }
        if (isLast) {
            receiver.finish(RepeatHandle.UNSUPPORTED);
            source.close();
        } else {
            run();
        }
    }

    private void consumePage(Page page) {
        Iterator<Row> it = page.bucket().iterator();
        consumeIt(it, page.isLast());
    }

    private void run(CompletableFuture<Page> pageFuture) {
        pageFuture
            .thenAccept(this::consumePage)
            .exceptionally(t -> { receiver.fail(t); return null; } );
    }

    public void run() {
        run(source.fetch());
    }
}
