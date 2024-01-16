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

package org.elasticsearch.index.query;

import java.util.Locale;

import org.apache.lucene.search.BooleanClause;

public enum Operator {
    OR,
    AND;

    public BooleanClause.Occur toBooleanClauseOccur() {
        return switch (this) {
            case OR -> BooleanClause.Occur.SHOULD;
            case AND -> BooleanClause.Occur.MUST;
        };
    }

    public static Operator fromString(String op) {
        return valueOf(op.toUpperCase(Locale.ROOT));
    }
}
