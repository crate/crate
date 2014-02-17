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

package org.cratedb.action.groupby.key;

import org.cratedb.action.groupby.GroupByRow;
import org.cratedb.action.sql.ParsedStatement;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public abstract class Rows<Other extends Rows> {

    public abstract GroupByRow getRow();

    public abstract void writeBucket(StreamOutput out, int idx) throws IOException;

    public abstract void readBucket(StreamInput in, int idx) throws IOException;

    public static Rows fromStream(ParsedStatement stmt,
            CacheRecycler cacheRecycler, StreamInput in) throws IOException {
        // note that this reads only into a single bucket
        Rows rows;
        if (stmt.isGlobalAggregate()){
            rows = new GlobalRows(1, stmt);
        } else {
            rows = new GroupTree(1, stmt, cacheRecycler);
        }
        rows.readBucket(in, 0);
        return rows;
    }

    public abstract void merge(Other other);

    public interface RowVisitor{
        public void visit(GroupByRow row);
    }

    public abstract void walk(RowVisitor visitor);
}
