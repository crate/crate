/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.lucene;

import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.lucene.match.CrateRegexCapabilities;
import io.crate.lucene.match.CrateRegexQuery;
import io.crate.metadata.Reference;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.lucene.BytesRefs;

class RegexMatchQueryCaseInsensitive extends CmpQuery {

    @Override
    public Query apply(Function input, LuceneQueryBuilder.Context context) {
        Tuple<Reference, Literal> prepare = prepare(input);
        if (prepare == null) {
            return null;
        }
        String fieldName = prepare.v1().column().fqn();
        Object value = prepare.v2().value();

        if (value instanceof BytesRef) {
            return new CrateRegexQuery(
                new Term(fieldName, BytesRefs.toBytesRef(value)),
                CrateRegexCapabilities.FLAG_CASE_INSENSITIVE | CrateRegexCapabilities.FLAG_UNICODE_CASE);
        }
        throw new IllegalArgumentException("Can only use ~* with patterns of type string");
    }
}
