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

import java.util.regex.Pattern;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;

import io.crate.expression.symbol.Function;
import io.crate.lucene.match.CrateRegexQuery;


class RegexMatchQueryCaseInsensitive implements FunctionToQuery {

    @Override
    public Query apply(Function input, LuceneQueryBuilder.Context context) {
        RefAndLiteral refAndLiteral = RefAndLiteral.of(input);
        if (refAndLiteral == null) {
            return null;
        }
        String fieldName = refAndLiteral.reference().column().fqn();
        Object value = refAndLiteral.literal().value();

        if (value instanceof String str) {
            return new CrateRegexQuery(
                new Term(fieldName, str), Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE
            );
        }
        throw new IllegalArgumentException("Can only use ~* with patterns of type string");
    }
}
