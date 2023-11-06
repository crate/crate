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

package io.crate.types;

import java.util.List;

import org.apache.lucene.search.Query;
import org.jetbrains.annotations.Nullable;

/**
 * For types which can be stored in Lucene and support optimized equality related queries
 **/
public interface EqQuery<T> {

    // NOTE: cannot utilize IndexOrDocValuesQuery since it is counted as 2 clauses
    // (one for indexQuery and the other for dvQuery) causing TooManyClauses exception
    // when exceeding the limit (indices.query.bool.max_clause_count).
    // https://github.com/crate/crate/pull/14527#discussion_r1295569395
    @Nullable
    Query termQuery(String field, T value, boolean hasDocValues, boolean isIndexed);

    // NOTE: cannot utilize IndexOrDocValuesQuery since it is counted as 2 clauses
    // (one for indexQuery and the other for dvQuery) causing TooManyClauses exception
    // when exceeding the limit (indices.query.bool.max_clause_count).
    // https://github.com/crate/crate/pull/14527#discussion_r1295569395
    @Nullable
    Query rangeQuery(String field,
                     T lowerTerm,
                     T upperTerm,
                     boolean includeLower,
                     boolean includeUpper,
                     boolean hasDocValues,
                     boolean isIndexed);

    @Nullable
    Query termsQuery(String field, List<T> nonNullValues, boolean hasDocValues, boolean isIndexed);
}
