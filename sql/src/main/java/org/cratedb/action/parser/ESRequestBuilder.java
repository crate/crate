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

import org.cratedb.Constants;
import org.cratedb.action.import_.ImportRequest;
import org.cratedb.action.sql.ParsedStatement;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.fetch.source.FetchSourceContext;

import java.io.IOException;

/**
 * The ESRequestBuilder can be used to build all sorts of ES Request from a {@link ParsedStatement}
 */
@Deprecated
public class ESRequestBuilder {

    private final ParsedStatement stmt;

    public ESRequestBuilder(ParsedStatement stmt) {
        this.stmt = stmt;
    }

    public CreateIndexRequest buildCreateIndexRequest() {
        CreateIndexRequest request = new CreateIndexRequest(stmt.tableName());
        request.settings(stmt.indexSettings);
        request.mapping(Constants.DEFAULT_MAPPING_TYPE, stmt.indexMapping);

        return request;
    }

    public DeleteIndexRequest buildDeleteIndexRequest() {
        return new DeleteIndexRequest(stmt.tableName());
    }

    /**
    * Used for setting custom analyzers
    * @return
    */
    public ClusterUpdateSettingsRequest buildClusterUpdateSettingsRequest() {
        ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest();
        request.persistentSettings(stmt.createAnalyzerSettings);
        return request;
    }
}
