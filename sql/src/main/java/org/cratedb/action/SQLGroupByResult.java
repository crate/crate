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

import org.cratedb.action.groupby.key.Rows;
import org.cratedb.action.sql.ParsedStatement;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;


/**
 * Result of a group by operation.
 * Each key represents a row for the SQLResponse.
 *
 * See {@link TransportDistributedSQLAction} for an overview of the workflow how the SQLGroupByResult is used.
 */
public class SQLGroupByResult {

    private int reducerIdx;
    private Rows rows;


    public SQLGroupByResult(int reducerIdx, Rows rows) {
        this.rows = rows;
        this.reducerIdx = reducerIdx;
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(reducerIdx);
        rows.writeBucket(out, reducerIdx);
    }

    public static SQLGroupByResult readSQLGroupByResult(ParsedStatement stmt,
                                                        CacheRecycler cacheRecycler,
                                                        StreamInput in) throws IOException {
        int reducerIdx = in.readInt();
        Rows rows = Rows.fromStream(stmt, cacheRecycler, in);
        return new SQLGroupByResult(reducerIdx, rows);
    }

    public Rows rows() {
        return rows;
    }
}
