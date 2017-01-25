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

package io.crate.executor;

import com.google.common.base.Function;
import org.elasticsearch.action.ActionListener;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class MultiActionListener<SingleResponse, FinalResponse> implements ActionListener<SingleResponse> {

    private final AtomicInteger counter;
    private final AtomicReference<Exception> lastExceptions = new AtomicReference<>(null);
    private final Function<List<SingleResponse>, FinalResponse> mergeFunction;
    private final ActionListener<? super FinalResponse> actionListener;
    private final ArrayList<SingleResponse> results;

    public MultiActionListener(int numResponses,
                               Function<List<SingleResponse>, FinalResponse> mergeFunction,
                               ActionListener<? super FinalResponse> actionListener) {
        this.mergeFunction = mergeFunction;
        this.actionListener = actionListener;
        results = new ArrayList<>(numResponses);
        counter = new AtomicInteger(numResponses);
    }

    @Override
    public void onResponse(SingleResponse response) {
        results.add(response);
        countdown();
    }

    @Override
    public void onFailure(Exception e) {
        lastExceptions.set(e);
        countdown();
    }

    private void countdown() {
        if (counter.decrementAndGet() == 0) {
            Exception e = lastExceptions.get();
            if (e == null) {
                actionListener.onResponse(mergeFunction.apply(results));
            } else {
                actionListener.onFailure(e);
            }
        }
    }
}
