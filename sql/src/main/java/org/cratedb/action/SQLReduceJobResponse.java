/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package org.cratedb.action;

import org.cratedb.DataType;
import org.cratedb.action.groupby.GroupByRow;
import org.cratedb.action.sql.ParsedStatement;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

public class SQLReduceJobResponse extends ActionResponse {

    private final ParsedStatement parsedStatement;
    private Collection<GroupByRow> rows;

    public SQLReduceJobResponse(ParsedStatement parsedStatement) {
        this.parsedStatement = parsedStatement;
    }

    public SQLReduceJobResponse(Collection<GroupByRow> rows, ParsedStatement stmt) {
        this(stmt);
        this.rows = rows;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int resultLength = in.readVInt();
        if (resultLength == 0) {
            return;
        }
        rows = new ArrayList<>(resultLength);
        if (parsedStatement.hasGroupBy()){
            DataType.Streamer[] streamers = parsedStatement.getGroupKeyStreamers();
            for (int i = 0; i < resultLength; i++) {
                rows.add(GroupByRow.readGroupByRow(parsedStatement, streamers, in));
            }
        } else {
            for (int i = 0; i < resultLength; i++) {
                GroupByRow row = new GroupByRow();
                row.readStates(in, parsedStatement);
                rows.add(row);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (rows == null || rows.size() == 0) {
            out.writeVInt(0);
            return;
        }
        out.writeVInt(rows.size());

        if (parsedStatement.hasGroupBy()){
            DataType.Streamer[] streamers = parsedStatement.getGroupKeyStreamers();
            for (GroupByRow row : rows) {
                row.writeTo(streamers, parsedStatement, out);
            }
        } else {
            for (GroupByRow row : rows) {
                row.writeStates(out, parsedStatement);
            }
        }
    }

    public Collection<GroupByRow> rows() {
        return rows;
    }
}
