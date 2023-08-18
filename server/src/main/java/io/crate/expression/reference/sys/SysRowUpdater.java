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

package io.crate.expression.reference.sys;

import io.crate.data.Input;
import io.crate.metadata.ColumnIdent;
import io.crate.execution.engine.collect.NestableCollectExpression;
import io.crate.execution.engine.collect.CollectExpression;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * A Component which allows for updating rows on a sys table.
 *
 * @param <TRow> The type of the underlying row object.
 */
public interface SysRowUpdater<TRow> {

    /**
     * Returns the row for the given identifier object.
     *
     * @param id the unique identifier of the row that should be returned
     * @return an object representing the row
     */
    TRow getRow(Object id);

    /**
     * Returns a consumer which can be used to write to a specific column of the table.
     *
     * Writing is done by calling the returned writer with the row that should be written to and an input which is used
     * as value.
     *
     * @param ci the column ident of the column that should be written to.
     * @return a consumer which takes a row object and an input as value
     */
    BiConsumer<TRow, Input<?>> getWriter(ColumnIdent ci);

    /**
     * Returns a new row writer which allows for updating multiple columns at once.
     *
     * The returned writer takes an id object as argument and uses {@link #getRow(Object)} to retrieve the row to write on.
     * When called the values of the given list of inputs gets evaluated and written on the given columns by using
     * writers returned by {@link #getWriter(ColumnIdent)}.
     *
     * @param idents the columns to be updated
     * @param values the inputs providing the values
     * @param expressions a list of row based expressions where {@link CollectExpression#setNextRow(TRow)} will be called.
     * @return a consumer which writes the values upon its invocation
     */
    default Consumer<Object> newRowWriter(List<ColumnIdent> idents, List<Input<?>> values, Collection<NestableCollectExpression<?, ?>> expressions) {
        assert idents.size() == values.size() : "the number of idents needs to match the number of values";
        List<BiConsumer<TRow, Input<?>>> writers = idents.stream().map(this::getWriter).toList();
        return id -> {
            TRow row = getRow(id);
            Iterator<Input<?>> iter = values.iterator();
            for (CollectExpression<?, ?> expression : expressions) {
                //noinspection unchecked
                ((CollectExpression<TRow, ?>) expression).setNextRow(row);
            }
            for (BiConsumer<TRow, Input<?>> writer : writers) {
                writer.accept(row, iter.next());
            }
        };
    }
}
