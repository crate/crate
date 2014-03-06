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

package io.crate.client.action.export;

import io.crate.action.export.ExportAction;
import io.crate.action.export.ExportRequest;
import io.crate.action.export.ExportResponse;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.internal.InternalClient;

public class ExportRequestBuilder extends ActionRequestBuilder<ExportRequest, ExportResponse, ExportRequestBuilder> {

    public ExportRequestBuilder(Client client) {
        super((InternalClient) client, new ExportRequest());
    }

    @Override
    protected void doExecute(ActionListener<ExportResponse> listener) {
        ((Client)client).execute(ExportAction.INSTANCE, request, listener);
    }

    public ExportRequestBuilder setIndices(String ... indices) {
        request.indices(indices);
        return this;
    }
}