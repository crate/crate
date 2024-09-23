/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.action;

import java.util.function.Supplier;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.ActiveShardsObserver;
import org.elasticsearch.action.support.master.AcknowledgedResponse;

import io.crate.common.unit.TimeValue;

public final class ActionListeners {

    private ActionListeners() {
    }

    public static <T extends AcknowledgedResponse> ActionListener<T> waitForShards(ActionListener<T> delegate,
                                                                                   ActiveShardsObserver observer,
                                                                                   TimeValue timeout,
                                                                                   Runnable onShardsNotAcknowledged,
                                                                                   Supplier<String[]> indices) {

        return ActionListener.wrap(
            resp -> {
                if (resp.isAcknowledged()) {
                    observer.waitForActiveShards(
                        indices.get(),
                        ActiveShardCount.DEFAULT,
                        timeout,
                        shardsAcknowledged -> {
                            if (!shardsAcknowledged) {
                                onShardsNotAcknowledged.run();
                            }
                            delegate.onResponse(resp);
                        },
                        delegate::onFailure
                    );
                } else {
                    delegate.onResponse(resp);
                }
            },
            delegate::onFailure
        );
    }
}
