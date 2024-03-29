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

package io.crate.execution.support;

import static io.crate.data.SentinelRow.SENTINEL;

import java.util.function.BiConsumer;
import java.util.function.Function;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.jetbrains.annotations.NotNull;

import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;

public class OneRowActionListener<Response> implements ActionListener<Response>, BiConsumer<Response, Throwable> {

    private final RowConsumer consumer;
    private final Function<? super Response, ? extends Row> toRowFunction;

    public static OneRowActionListener<? super AcknowledgedResponse> oneIfAcknowledged(RowConsumer consumer) {
        return new OneRowActionListener<>(consumer, resp -> resp.isAcknowledged() ? new Row1(1L) : new Row1(0L));
    }

    public OneRowActionListener(RowConsumer consumer, Function<? super Response, ? extends Row> toRowFunction) {
        this.consumer = consumer;
        this.toRowFunction = toRowFunction;
    }

    @Override
    public void onResponse(Response response) {
        final Row row;
        try {
            row = toRowFunction.apply(response);
        } catch (Throwable t) {
            consumer.accept(null, t);
            return;
        }
        consumer.accept(InMemoryBatchIterator.of(row, SENTINEL), null);
    }

    @Override
    public void onFailure(@NotNull Exception e) {
        consumer.accept(null, e);
    }

    @Override
    public void accept(Response response, Throwable t) {
        if (t == null) {
            onResponse(response);
        } else {
            consumer.accept(null, t);
        }
    }
}
