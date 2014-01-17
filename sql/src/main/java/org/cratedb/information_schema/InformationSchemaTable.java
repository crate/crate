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

package org.cratedb.information_schema;

import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.action.sql.SQLResponse;
import org.cratedb.lucene.LuceneFieldMapper;
import org.cratedb.sql.CrateException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;

public interface InformationSchemaTable {
    public Iterable<String> cols();
    public LuceneFieldMapper fieldMapper();

    /**
     * initialize this table and its resources
     */
    public void init() throws CrateException;
    public boolean initialized();

    /**
     * close this table and release its resources
     * @throws CrateException
     */
    public void close() throws CrateException;

    /**
     * Query this table for results
     * @param stmt the statement containing the parsed query
     * @param listener the listener to be called on success or failure
     * @param requestStartedTime point in time the request was started at
     */
    public void query(ParsedStatement stmt, ActionListener<SQLResponse> listener, long requestStartedTime);

    /**
     * extract and index table data from the current ClusterState
     *
     * not synchronized, please synchronize when using to stay threadsafe
     * @param clusterState
     */
    public void index(ClusterState clusterState);
}
