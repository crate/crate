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

package io.crate.execution.engine.collect.stats;

import java.util.Iterator;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.Message;

import io.crate.expression.ExpressionsInput;

public final class FilteredLogSink<T> implements LogSink<T> {

    /**
     * This logger name is documented in the docs for the {@link JobsLogService#STATS_JOBS_LOG_PERSIST_FILTER} setting.
     * Take care when changing the name.
     */
    private static final Logger STATEMENT_LOGGER = LogManager.getLogger("StatementLog");
    private final ExpressionsInput<T, Boolean> memoryFilter;
    private final ExpressionsInput<T, Boolean> persistFilter;
    private final Function<T, Message> createLogMessage;
    final LogSink<T> delegate;

    FilteredLogSink(ExpressionsInput<T, Boolean> memoryFilter,
                    ExpressionsInput<T, Boolean> persistFilter,
                    Function<T, Message> createLogMessage,
                    LogSink<T> delegate) {
        this.memoryFilter = memoryFilter;
        this.persistFilter = persistFilter;
        this.createLogMessage = createLogMessage;
        this.delegate = delegate;
    }

    @Override
    public void add(T item) {
        Boolean recordToMemory = memoryFilter.value(item);
        if (recordToMemory != null && recordToMemory) {
            delegate.add(item);
        }
        Boolean recordToPersistentLog = persistFilter.value(item);
        if (recordToPersistentLog != null && recordToPersistentLog && STATEMENT_LOGGER.isInfoEnabled()) {
            STATEMENT_LOGGER.info(createLogMessage.apply(item));
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
    public Iterator<T> iterator() {
        return delegate.iterator();
    }
}
