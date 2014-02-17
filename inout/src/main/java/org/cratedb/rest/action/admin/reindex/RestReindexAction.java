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

package org.cratedb.rest.action.admin.reindex;

import static org.elasticsearch.rest.RestRequest.Method.POST;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestController;

import org.cratedb.action.reindex.ReindexAction;
import org.cratedb.action.searchinto.SearchIntoRequest;
import org.cratedb.action.searchinto.SearchIntoResponse;
import org.cratedb.client.action.searchinto.SearchIntoRequestBuilder;
import org.cratedb.rest.action.admin.searchinto.RestSearchIntoAction;

/**
 * Rest action for the _reindex end points. Does the _search_into action to
 * the own index with the defined fields _id and _source.
 *
 * Does a re-index to either all indexes, a specified index or a specific type of
 * a specified index.
 */
public class RestReindexAction extends RestSearchIntoAction {

    @Inject
    public RestReindexAction(Settings settings, Client client, RestController controller) {
        super(settings, client, controller);
    }

    @Override
    protected void registerHandlers(RestController controller) {
        controller.registerHandler(POST, "/_reindex", this);
        controller.registerHandler(POST, "/{index}/_reindex", this);
        controller.registerHandler(POST, "/{index}/{type}/_reindex", this);
    }

    @Override
    protected Action<SearchIntoRequest, SearchIntoResponse, SearchIntoRequestBuilder> action() {
        return ReindexAction.INSTANCE;
    }
}
