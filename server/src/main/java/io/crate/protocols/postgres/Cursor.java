/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.protocols.postgres;

import java.util.List;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import io.crate.action.sql.PreparedStmt;
import io.crate.action.sql.ResultReceiver;
import io.crate.action.sql.RowConsumerToResultReceiver;
import io.crate.data.BatchIterator;
import io.crate.data.Row;

/**
 * Cursor is created by Declare and reused throughout successive Fetches then removed when Close is called.
 */
public class Cursor extends Portal {

    public enum State {
        Declare, Fetch
    }

    private State state;

    Cursor(String portalName,
           PreparedStmt preparedStmt,
           List<Object> params,
           @Nullable FormatCodes.FormatCode[] resultFormatCodes) {
        super(portalName, preparedStmt, params, resultFormatCodes);
        this.state = State.Declare;
    }

    public void bindFetch(PreparedStmt preparedStmt) {
        // later on preparedStmt.analyzedStatement.outputs() decides RowCountReceiver/ResultSetReciever
        updatePreparedStmt(preparedStmt);
        this.state = State.Fetch;

        // ex) fetch x from cursor; fetch y from cursor;
        // if rowCount is not reset, y is affected by x because the consumer is reused
        var activeConsumer = this.activeConsumer();
        if (activeConsumer != null) {
            activeConsumer.resetRowCount();
        }
    }

    public State state() {
        return state;
    }

    public void setActiveConsumer(ResultReceiver<?> resultReceiver,
                                  int maxRows,
                                  JobsLogsUpdateListener jobsLogsUpdateListener) {
        setActiveConsumer(
            new StatefulRowConsumerToResultReceiver(
                resultReceiver,
                maxRows,
                jobsLogsUpdateListener.stmtUpdateListener(),
                jobsLogsUpdateListener.executionEndListener(),
                this
            )
        );
    }

    private static class StatefulRowConsumerToResultReceiver extends RowConsumerToResultReceiver {

        private final Cursor cursor;
        private final Consumer<String> onStatementUpdate;

        public StatefulRowConsumerToResultReceiver(ResultReceiver<?> resultReceiver,
                                                   int maxRows,
                                                   Consumer<String> onStatementUpdate,
                                                   Consumer<Throwable> onCompletion,
                                                   Cursor cursor) {
            super(resultReceiver, maxRows, onCompletion);
            this.cursor = cursor;
            this.onStatementUpdate = onStatementUpdate;
        }

        @Override
        public void accept(BatchIterator<Row> iterator, @Nullable Throwable failure) {
            if (failure != null) {
                handleFailure(iterator, failure);
            } else {
                if (cursor.state() == State.Declare) {
                    // Builds the pipeline for future 'fetches' but does not consume.
                    // This will ensure the 'fetches' can only retrieve rows available at the time of 'declare'.
                    suspend(iterator);
                } else if (cursor.state() == State.Fetch) {
                    onStatementUpdate.accept(cursor.preparedStmt().rawStatement());
                    consumeIt(iterator);
                }
            }
        }

        @Override
        public void resume() {
            onStatementUpdate.accept(cursor.preparedStmt().rawStatement());
            super.resume();
        }
    }
}
