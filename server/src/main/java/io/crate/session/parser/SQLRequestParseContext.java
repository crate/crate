/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.session.parser;

import java.util.List;

/**
 * Context for information gathered by parsing an XContent based sql request
 */
public final class SQLRequestParseContext {

    private String stmt;
    private List<Object> args;
    private List<List<Object>> bulkArgs;

    public String stmt() {
        return stmt;
    }

    public void stmt(String stmt) {
        this.stmt = stmt;
    }

    public List<Object> args() {
        return args;
    }

    public void args(List<Object> args) {
        this.args = args;
    }

    public List<List<Object>> bulkArgs() {
        return bulkArgs;
    }

    public void bulkArgs(List<List<Object>> bulkArgs) {
        this.bulkArgs = bulkArgs;
    }
}
