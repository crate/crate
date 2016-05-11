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

package io.crate.concurrent;

import com.google.common.base.MoreObjects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class CompletionMultiListener implements CompletionListener {

    private final List<CompletionListener> listeners = new ArrayList<>();

    public static CompletionListener merge(@Nullable CompletionListener currentListener,
                                           CompletionListener newListener) {
        if (currentListener == null || currentListener == NO_OP) {
            return newListener;
        }
        if (currentListener instanceof CompletionMultiListener) {
            ((CompletionMultiListener) currentListener).listeners.add(newListener);
            return currentListener;
        }
        return new CompletionMultiListener(currentListener, newListener);
    }

    private CompletionMultiListener(CompletionListener currentListener, CompletionListener newListener) {
        listeners.add(currentListener);
        listeners.add(newListener);
    }

    @Override
    public void onSuccess(@Nullable CompletionState state) {
        for (CompletionListener listener : listeners) {
            listener.onSuccess(state);
        }
    }

    @Override
    public void onFailure(@Nonnull Throwable throwable) {
        for (CompletionListener listener : listeners) {
            listener.onFailure(throwable);
        }
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("listeners", listeners)
            .toString();
    }

}
