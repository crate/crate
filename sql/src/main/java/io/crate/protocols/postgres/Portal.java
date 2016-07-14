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

package io.crate.protocols.postgres;

import io.crate.action.sql.ResultReceiver;
import io.crate.action.sql.RowReceiverToResultReceiver;
import io.crate.analyze.Analysis;
import io.crate.analyze.Analyzer;
import io.crate.concurrent.CompletionListener;
import io.crate.operation.collect.StatsTables;
import io.crate.planner.Planner;
import io.crate.sql.tree.Statement;
import io.crate.types.DataType;

import javax.annotation.Nullable;
import java.util.List;

public interface Portal {

    enum PortalType {
        SIMPLE, BULK
    }

    PortalType type();

    Object[] getArgs();

    Analysis getAnalysis(String statementName);

    Analysis getLastAnalysis();

    FormatCodes.FormatCode[] getResultFormatCodes();

    List<? extends DataType> getOutputTypes();

    Statement getStatement(String statementName);

    String getLastQuery();

    void addQuery(String statementName, String query);

    void addStatement(String statementName, Statement statement);

    void addAnalysis(String statementName, Analysis analysis);

    void addParams(List<Object> params);

    void addResultFormatCodes(@Nullable FormatCodes.FormatCode[] resultFormatCodes);

    void addOutputTypes(List<? extends DataType> outputTypes);

    void addResultReceiver(ResultReceiver resultReceiver);

    void setRowReceiver(RowReceiverToResultReceiver rowReceiver);

    void setMaxRows(int maxRows);

    void execute(Analyzer analyzer, Planner planner, StatsTables statsTables, CompletionListener listener);

    void close();
}
