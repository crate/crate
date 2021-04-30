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
import io.crate.lucene.match.CrateRegexQuery;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.util.automaton.RegExp;

import static io.crate.expression.RegexpFlags.isPcrePattern;

class RegexpMatchQuery implements FunctionToQuery {

    private static Query toLuceneRegexpQuery(String fieldName, String value) {
        return new ConstantScoreQuery(
            new RegexpQuery(new Term(fieldName, value), RegExp.ALL));
    }

    @Override
    public Query apply(Function input, LuceneQueryBuilder.Context context) {
        RefAndLiteral refAndLiteral = RefAndLiteral.of(input);
        if (refAndLiteral == null) {
            return null;
        }
        String fieldName = refAndLiteral.reference().column().fqn();
        String pattern = (String) refAndLiteral.literal().value();
        if (pattern == null) {
            // cannot build query using null pattern value
            return null;
        }
        if (isPcrePattern(pattern)) {
            return new CrateRegexQuery(new Term(fieldName, pattern));
        } else {
            return toLuceneRegexpQuery(fieldName, pattern);
        }
    }
}
