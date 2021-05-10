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

package io.crate.lucene;

import io.crate.expression.symbol.Function;
import org.apache.lucene.search.Query;

import javax.annotation.Nullable;
import java.io.IOException;

/**
 * interface for functions that can be used to generate a query from inner functions.
 * Has only a single method {@link #apply(Function, Function, LuceneQueryBuilder.Context)}
 * <p>
 * e.g. in a query like
 * <pre>
 *     where distance(p1, 'POINT (10 20)') = 20
 * </pre>
 * <p>
 * The first parameter (parent) would be the "eq" function.
 * The second parameter (inner) would be the "distance" function.
 * <p>
 * The returned Query must "contain" both the parent and inner functions.
 */
interface InnerFunctionToQuery {

    /**
     * returns a query for the given functions or null if it can't build a query.
     */
    @Nullable
    Query apply(Function parent, Function inner, LuceneQueryBuilder.Context context) throws IOException;
}
