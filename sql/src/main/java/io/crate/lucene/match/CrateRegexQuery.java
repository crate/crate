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

package io.crate.lucene.match;

import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.util.AttributeSource;

import java.io.IOException;
import java.util.Objects;


/**
 * Implements the regular expression term search query for patterns
 * of PCRE format (which include escape sequences and/or embedded flags).
 * It was formerly implemented by Lucene's deprecated RegexQuery and the
 * functionality is no longer available in newer Lucene versions.
 */
public class CrateRegexQuery extends MultiTermQuery {

    private Term term;

    /** Constructs a query for terms matching <code>term</code>. */
    public CrateRegexQuery(Term term) {
        super(term.field());
        this.term = term;
    }

    @Override
    protected FilteredTermsEnum getTermsEnum(Terms terms, AttributeSource atts) throws IOException {
        return new CrateRegexTermsEnum(terms.iterator(), term);
    }

    @Override
    public String toString(String field) {
        StringBuilder buffer = new StringBuilder();
        if (!term.field().equals(field)) {
            buffer.append(term.field());
            buffer.append(":");
        }
        buffer.append(term.text());
        return buffer.toString();
    }

    // hasCode() and equals() are required because Lucene caches the queries
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        CrateRegexQuery that = (CrateRegexQuery) o;
        return Objects.equals(term, that.term);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), term);
    }
}
