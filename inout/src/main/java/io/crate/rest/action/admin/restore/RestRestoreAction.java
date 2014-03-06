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

package io.crate.rest.action.admin.restore;

import io.crate.action.restore.RestoreAction;
import io.crate.rest.action.admin.import_.RestImportAction;
import org.elasticsearch.action.Action;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestController;

import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * Rest handler for _restore endpoint
 */
public class RestRestoreAction extends RestImportAction {

    @Inject
    public RestRestoreAction(Settings settings, Client client, RestController controller) {
        super(settings, client, controller);
    }

    @Override
    protected Action action() {
        return RestoreAction.INSTANCE;
    }

    @Override
    protected void registerHandlers(RestController controller) {
        controller.registerHandler(POST, "/_restore", this);
        controller.registerHandler(POST, "/{index}/_restore", this);
        controller.registerHandler(POST, "/{index}/{type}/_restore", this);
    }

}
