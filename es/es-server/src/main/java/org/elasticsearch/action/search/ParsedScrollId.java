/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.search;

class ParsedScrollId {

    public static final String QUERY_THEN_FETCH_TYPE = "queryThenFetch";

    public static final String QUERY_AND_FETCH_TYPE = "queryAndFetch";

    private final String source;

    private final String type;

    private final ScrollIdForNode[] context;

    ParsedScrollId(String source, String type, ScrollIdForNode[] context) {
        this.source = source;
        this.type = type;
        this.context = context;
    }

    public String getSource() {
        return source;
    }

    public String getType() {
        return type;
    }

    public ScrollIdForNode[] getContext() {
        return context;
    }
}
