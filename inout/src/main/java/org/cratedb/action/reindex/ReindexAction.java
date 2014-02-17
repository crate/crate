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

package org.cratedb.action.reindex;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.Client;

import org.cratedb.action.searchinto.SearchIntoRequest;
import org.cratedb.action.searchinto.SearchIntoResponse;
import org.cratedb.client.action.searchinto.SearchIntoRequestBuilder;

public class ReindexAction extends Action<SearchIntoRequest, SearchIntoResponse, SearchIntoRequestBuilder>{

    public static final ReindexAction INSTANCE = new ReindexAction();
    public static final String NAME = "el-crate-reindex";

    protected ReindexAction() {
        super(NAME);
    }

    @Override
    public SearchIntoRequestBuilder newRequestBuilder(Client client) {
        return new SearchIntoRequestBuilder(client);
    }

    @Override
    public SearchIntoResponse newResponse() {
        return new SearchIntoResponse();
    }

}
