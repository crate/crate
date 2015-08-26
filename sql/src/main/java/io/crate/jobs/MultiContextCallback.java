/*
 * Licensed to Crate.IO GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.jobs;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class MultiContextCallback implements ContextCallback {

    private final List<ContextCallback> callbacks = new ArrayList<>();

    public static ContextCallback merge(@Nullable ContextCallback currentCallback, ContextCallback newCallback) {
        if (currentCallback == null || currentCallback == ContextCallback.NO_OP) {
            return newCallback;
        }
        if (currentCallback instanceof MultiContextCallback) {
            ((MultiContextCallback) currentCallback).callbacks.add(newCallback);
            return currentCallback;
        }
        return new MultiContextCallback(currentCallback, newCallback);
    }

    public MultiContextCallback(ContextCallback currentCallback, ContextCallback newCallback) {
        callbacks.add(currentCallback);
        callbacks.add(newCallback);
    }

    @Override
    public void onClose(@Nullable Throwable error, long bytesUsed) {
        for (ContextCallback callback : callbacks) {
            callback.onClose(error, bytesUsed);
        }
    }

    @Override
    public void keepAlive() {
        for (ContextCallback callback : callbacks) {
            callback.keepAlive();
        }
    }
}
