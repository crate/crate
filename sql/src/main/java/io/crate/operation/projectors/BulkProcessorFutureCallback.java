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

import com.google.common.util.concurrent.FutureCallback;
import io.crate.core.collections.Row1;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.BitSet;
import java.util.concurrent.atomic.AtomicBoolean;

class BulkProcessorFutureCallback implements FutureCallback<BitSet> {
    private final AtomicBoolean failed;
    private final RowReceiver rowReceiver;

    public BulkProcessorFutureCallback(AtomicBoolean failed, RowReceiver rowReceiver) {
        this.failed = failed;
        this.rowReceiver = rowReceiver;
    }

    @Override
    public void onSuccess(@Nullable BitSet result) {
        if (!failed.get()) {
            long rowCount = result == null ? 0 : result.cardinality();
            rowReceiver.setNextRow(new Row1(rowCount));
            rowReceiver.finish();
        }
    }

    @Override
    public void onFailure(@Nonnull Throwable t) {
        if (!failed.get()) {
            rowReceiver.fail(t);
        }
    }
}
