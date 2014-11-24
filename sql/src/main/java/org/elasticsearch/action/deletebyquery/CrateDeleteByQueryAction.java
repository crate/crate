/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.elasticsearch.action.deletebyquery;

import org.elasticsearch.action.ClientAction;
import org.elasticsearch.client.Client;

public class CrateDeleteByQueryAction extends ClientAction<DeleteByQueryRequest, DeleteByQueryResponse, DeleteByQueryRequestBuilder> {

    public static final CrateDeleteByQueryAction INSTANCE = new CrateDeleteByQueryAction();
    public static final String NAME = "crate:indices:data/write/delete/by_query";

    private CrateDeleteByQueryAction() {
        super(NAME);
    }

    @Override
    public DeleteByQueryResponse newResponse() {
        return new DeleteByQueryResponse();
    }

    @Override
    public DeleteByQueryRequestBuilder newRequestBuilder(Client client) {
        return new DeleteByQueryRequestBuilder(client);
    }
}
