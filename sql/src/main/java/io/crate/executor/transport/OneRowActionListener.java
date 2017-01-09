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

package io.crate.executor.transport;

import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;
import io.crate.core.collections.Row;
import io.crate.operation.projectors.RepeatHandle;
import io.crate.operation.projectors.RowReceiver;
import org.elasticsearch.action.ActionListener;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class OneRowActionListener<Response> implements ActionListener<Response>, FutureCallback<Response> {

    private final RowReceiver rowReceiver;
    private final Function<? super Response, ? extends Row> toRowFunction;

    public OneRowActionListener(RowReceiver rowReceiver, Function<? super Response, ? extends Row> toRowFunction) {
        this.rowReceiver = rowReceiver;
        this.toRowFunction = toRowFunction;
    }

    @Override
    public void onResponse(Response response) {
        rowReceiver.setNextRow(toRowFunction.apply(response));
        rowReceiver.finish(RepeatHandle.UNSUPPORTED);
    }

    @Override
    public void onSuccess(@Nullable Response result) {
        onResponse(result);
    }

    @Override
    public void onFailure(@Nonnull Throwable e) {
        rowReceiver.fail(e);
    }

    @Override
    public void onFailure(Exception e) {
        rowReceiver.fail(e);
    }
}
