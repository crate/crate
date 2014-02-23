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

package org.cratedb.action.parser;

import org.cratedb.action.sql.NodeExecutionContext;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.action.sql.SQLResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;

/**
 * The SQLResponseBuilder can be used to convert ES Responses into a {@link SQLResponse}
 */
public class SQLResponseBuilder {

    private final ParsedStatement stmt;

    public SQLResponseBuilder(NodeExecutionContext context, ParsedStatement stmt) {
        this.stmt = stmt;
    }

    public SQLResponse buildResponse(CreateIndexResponse createIndexResponse,
                                     long requestStartedTime) {
        return buildEmptyResponse(0, requestStartedTime);
    }

    public SQLResponse buildResponse(DeleteIndexResponse deleteIndexResponse,
                                     long requestStartedTime) {
        return buildEmptyResponse(0, requestStartedTime);
    }

    private SQLResponse buildEmptyResponse(int rowCount, long requestStartedTime) {
        SQLResponse response = new SQLResponse();
        response.cols(stmt.cols());
        response.rows(new Object[0][0]);
        response.rowCount(rowCount);
        response.requestStartedTime(requestStartedTime);

        return response;
    }

    public SQLResponse buildResponse(ClusterUpdateSettingsResponse clusterUpdateSettingsResponse,
                                     long requestStartedTime) {
        return buildEmptyResponse(0, requestStartedTime);
    }
}
