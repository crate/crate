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

package io.crate.execution.engine.collect.stats;

import io.crate.data.Input;
import io.crate.execution.engine.collect.NestableCollectExpression;
import io.crate.expression.InputCondition;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.List;

public final class FilteredLogSink<T> implements LogSink<T> {

    private final Input<Boolean> filter;
    private final List<NestableCollectExpression<T, ?>> filterExpressions;
    final LogSink<T> delegate;

    FilteredLogSink(Input<Boolean> filter,
                    List<NestableCollectExpression<T, ?>> filterExpressions,
                    LogSink<T> delegate) {
        this.filter = filter;
        this.filterExpressions = filterExpressions;
        this.delegate = delegate;
    }

    @Override
    public void add(T item) {
        for (int i = 0; i < filterExpressions.size(); i++) {
            filterExpressions.get(i).setNextRow(item);
        }
        if (InputCondition.matches(filter)) {
            delegate.add(item);
        }
    }

    @Override
    public void addAll(Iterable<T> iterable) {
        iterable.forEach(this::add);
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    @Nonnull
    public Iterator<T> iterator() {
        return delegate.iterator();
    }
}
