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

package io.crate.action.sql.parser;

/**
 * Context for information gathered by parsing an XContent based sql request
 */
public class SQLXContentSourceContext {

    private String stmt;
    private Object[] args;
    private Object[][] bulkArgs;

    public String stmt() {
        return stmt;
    }

    public void stmt(String stmt) {
        this.stmt = stmt;
    }

    public Object[] args() {
        return args;
    }

    public void args(Object[] args) {
        this.args = args;
    }

    public Object[][] bulkArgs() {
        return bulkArgs;
    }

    public void bulkArgs(Object[][] bulkArgs) {
        this.bulkArgs = bulkArgs;
    }
}
