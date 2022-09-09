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

import io.crate.action.sql.PreparedStmt;
import io.crate.action.sql.RowConsumerToResultReceiver;
import io.crate.analyze.AnalyzedStatement;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Cursor is created by Declare and reused throughout successive Fetches then removed when Close is called.
 */
public class Cursor extends Portal {

    private enum State {
        Declare, Fetch;
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

    @Override
    public void setActiveConsumer(RowConsumerToResultReceiver consumer) {
        super.setActiveConsumer(consumer);
        if (consumer != null) {
            consumer.completionFuture().whenComplete((r, t) -> {
                if (this.state == State.Declare) {
                    super.setActiveConsumer(null);
                }
            });
        }
    }
}
