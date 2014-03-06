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

package io.crate.action.sql;

import io.crate.action.sql.parser.SQLXContentSourceContext;
import io.crate.action.sql.parser.SQLXContentSourceParser;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.internal.InternalClient;
import org.elasticsearch.common.bytes.BytesReference;

public class SQLRequestBuilder extends ActionRequestBuilder<SQLRequest, SQLResponse, SQLRequestBuilder> {

    private SQLXContentSourceParser parser;

    public SQLRequestBuilder(Client client) {
        super((InternalClient) client, new SQLRequest());
    }

    /**
     * Executes the built request on the client
     *
     * @param listener
     */
    @Override
    protected void doExecute(ActionListener<SQLResponse> listener) {
        ((Client) client).execute(SQLAction.INSTANCE, request, listener);
    }

    public SQLRequestBuilder source(BytesReference source) {
        SQLXContentSourceContext context = new SQLXContentSourceContext();
        parser = new SQLXContentSourceParser(context);
        parser.parseSource(source);
        request.stmt(context.stmt());
        request.args(context.args());
        return this;
    }

}
