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

package io.crate.operation.union;

import io.crate.core.collections.PrependedRow;
import io.crate.core.collections.Row;
import io.crate.operation.projectors.RepeatHandle;
import io.crate.operation.projectors.Requirement;
import io.crate.operation.projectors.ResumeHandle;
import io.crate.operation.projectors.RowReceiver;

import java.util.Set;

/**
 * Wrap a {@link RowReceiver} and prepend each {@link Row} with an inputId
 * to be able to distinguish between rows received by different sources.
 *
 * eg: Union queries
 */
public class PrependInputIdRowReceiver implements RowReceiver {

    private final RowReceiver delegate;
    private final byte inputId;

    public PrependInputIdRowReceiver(RowReceiver delegate, byte inputId) {
        this.delegate = delegate;
        this.inputId = inputId;
    }

    @Override
    public Result setNextRow(Row row) {
        return delegate.setNextRow(new PrependedRow(row, inputId));
    }

    @Override
    public void pauseProcessed(ResumeHandle resumeable) {
        delegate.pauseProcessed(resumeable);
    }

    @Override
    public void finish(RepeatHandle repeatable) {
        delegate.finish(repeatable);
    }

    @Override
    public void fail(Throwable throwable) {
        delegate.fail(throwable);
    }

    @Override
    public void kill(Throwable throwable) {
        delegate.kill(throwable);
    }

    @Override
    public Set<Requirement> requirements() {
        return delegate.requirements();
    }
}
