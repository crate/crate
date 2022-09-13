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
import io.crate.analyze.AnalyzedStatement;
import io.crate.data.BatchIterator;
import io.crate.data.Row;

/**
 * Cursor is created by Declare and reused throughout successive Fetches then removed when Close is called.
 */
public class Cursor extends Portal {

    public enum State {
        Declare, Fetch
    }

    private PreparedStmt preparedStmt; // hides Portal.preparedStmt
    private State state;

    Cursor(String portalName,
           PreparedStmt preparedStmt,
           List<Object> params,
           AnalyzedStatement analyzedStatement,
           @Nullable FormatCodes.FormatCode[] resultFormatCodes) {
        super(portalName, preparedStmt, params, analyzedStatement, resultFormatCodes);
        this.preparedStmt = preparedStmt;
        this.state = State.Declare;
    }

    public void bindFetch(PreparedStmt preparedStmt) {
        this.preparedStmt = preparedStmt;
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

    @Override
    public PreparedStmt preparedStmt() {
        return preparedStmt;
    }

    @Override
    public AnalyzedStatement analyzedStatement() {
        return preparedStmt.analyzedStatement();
    }

    public void close() {

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

    @Override
    protected void setActiveConsumer(RowConsumerToResultReceiver consumer) {
        super.setActiveConsumer(consumer);
        if (consumer != null) {
            consumer.completionFuture().whenComplete((r, t) -> super.setActiveConsumer(null));
        }
    }

    private static class StatefulRowConsumerToResultReceiver extends RowConsumerToResultReceiver {

        private final Cursor cursor;
        private final ResultReceiver<?> resultReceiver;
        private BatchIterator<Row> iterator;
        private final Consumer<String> onStatementUpdate;

        public StatefulRowConsumerToResultReceiver(ResultReceiver<?> resultReceiver,
                                                   int maxRows,
                                                   Consumer<String> onStatementUpdate,
                                                   Consumer<Throwable> onCompletion,
                                                   Cursor cursor) {
            super(resultReceiver, maxRows, onCompletion);
            this.cursor = cursor;
            this.resultReceiver = resultReceiver;
            this.onStatementUpdate = onStatementUpdate;
        }

        @Override
        public void accept(BatchIterator<Row> iterator, @Nullable Throwable failure) {
            this.iterator = iterator;
            if (cursor.state() == State.Declare) {
                resultReceiver.batchFinished();
            }
            if (cursor.state() == State.Fetch) {
                onStatementUpdate.accept(cursor.preparedStmt().rawStatement());
                super.accept(iterator, failure);
            }
        }

        @Override
        public boolean suspended() {
            if (cursor.state() == State.Declare) {
                return false;
            } else {
                return true;
            }
        }

        @Override
        public void resume() {
            onStatementUpdate.accept(cursor.preparedStmt().rawStatement());
            if (super.suspended()) {
                super.resume();
            } else {
                accept(iterator, null);
            }
        }
    }
}
