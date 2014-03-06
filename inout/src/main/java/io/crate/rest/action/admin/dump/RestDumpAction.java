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

package io.crate.rest.action.admin.dump;

import static org.elasticsearch.rest.RestRequest.Method.POST;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestController;

import io.crate.action.dump.DumpAction;
import io.crate.action.export.ExportRequest;
import io.crate.action.export.ExportResponse;
import io.crate.client.action.export.ExportRequestBuilder;
import io.crate.rest.action.admin.export.RestExportAction;

/**
 * Rest handler for _dump endpoint
 */
public class RestDumpAction extends RestExportAction {

    @Inject
    public RestDumpAction(Settings settings, Client client, RestController controller) {
        super(settings, client, controller);
    }

    @Override
    protected Action<ExportRequest, ExportResponse, ExportRequestBuilder> action() {
        return DumpAction.INSTANCE;
    }

    @Override
    protected void registerHandlers(RestController controller) {
        controller.registerHandler(POST, "/_dump", this);
        controller.registerHandler(POST, "/{index}/_dump", this);
        controller.registerHandler(POST, "/{index}/{type}/_dump", this);
    }

}
