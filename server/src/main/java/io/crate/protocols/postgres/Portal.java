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

package io.crate.protocols.postgres;

import io.crate.action.sql.PreparedStmt;
import io.crate.action.sql.ResultReceiver;
import io.crate.action.sql.RowConsumerToResultReceiver;
import io.crate.analyze.AnalyzedStatement;

import javax.annotation.Nullable;
import java.util.List;

public class Portal {

    private final String portalName;
    private PreparedStmt preparedStmt;
    private final List<Object> params;

    @Nullable
    private final FormatCodes.FormatCode[] resultFormatCodes;

    private RowConsumerToResultReceiver consumer;

    Portal(String portalName,
           PreparedStmt preparedStmt,
           List<Object> params,
           @Nullable FormatCodes.FormatCode[] resultFormatCodes) {
        this.portalName = portalName;
        this.preparedStmt = preparedStmt;
        this.params = params;
        this.resultFormatCodes = resultFormatCodes;
    }

    public String name() {
        return portalName;
    }

    public PreparedStmt preparedStmt() {
        return preparedStmt;
    }

    public List<Object> params() {
        return params;
    }

    @Nullable
    public FormatCodes.FormatCode[] resultFormatCodes() {
        return resultFormatCodes;
    }

    public AnalyzedStatement analyzedStatement() {
        return preparedStmt.analyzedStatement();
    }

    public void setActiveConsumer(ResultReceiver<?> resultReceiver, int maxRows, JobsLogsUpdateListener jobsLogsUpdateListener) {
        setActiveConsumer(new RowConsumerToResultReceiver(resultReceiver, maxRows, jobsLogsUpdateListener.executionEndListener()));
    }

    protected void setActiveConsumer(RowConsumerToResultReceiver consumer) {
        this.consumer = consumer;
    }

    @Nullable
    public RowConsumerToResultReceiver activeConsumer() {
        return consumer;
    }

    public void closeActiveConsumer() {
        if (consumer != null) {
            consumer.closeAndFinishIfSuspended();
        }
    }

    @Override
    public String toString() {
        return "Portal{" +
               "portalName=" + portalName +
               ", preparedStmt=" + preparedStmt.rawStatement() +
               '}';
    }

    protected void updatePreparedStmt(PreparedStmt preparedStmt) {
        this.preparedStmt = preparedStmt;
    }
}
