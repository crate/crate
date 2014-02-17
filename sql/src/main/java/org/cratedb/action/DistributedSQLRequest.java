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

package org.cratedb.action;

import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.action.sql.SQLRequest;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;

/**
 * Container that wraps a {@link SQLRequest} and a {@link ParsedStatement}
 *
 * This is used to pass the ParsedStatement to the {@link TransportDistributedSQLAction}
 * This is required because the {@link TransportDistributedSQLAction#execute(org.elasticsearch.action.ActionRequest, org.elasticsearch.action.ActionListener)}
 * method signature can't be extended.
 *
 * the DistributedSQLRequest is never sent over the wire, only locally passed around.
 */
public class DistributedSQLRequest extends ActionRequest {

    public SQLRequest sqlRequest;
    public ParsedStatement parsedStatement;

    public DistributedSQLRequest() {

    }

    public DistributedSQLRequest(SQLRequest request, ParsedStatement stmt) {
        sqlRequest = request;
        parsedStatement = stmt;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
