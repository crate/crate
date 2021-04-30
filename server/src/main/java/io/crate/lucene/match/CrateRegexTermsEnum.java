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

package io.crate.lucene.match;

import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;

/**
 * Subclass of FilteredTermEnum for enumerating all terms that match the
 * specified regular expression term using the specified regular expression
 * implementation.
 * <p>
 * Term enumerations are always ordered by Term.compareTo().  Each term in
 * the enumeration is greater than all that precede it.
 */
class CrateRegexTermsEnum extends FilteredTermsEnum {

    private CrateRegexCapabilities.JavaUtilRegexMatcher regexImpl;

    CrateRegexTermsEnum(TermsEnum tenum, Term term, int flags) {
        super(tenum);
        String text = term.text();
        this.regexImpl = CrateRegexCapabilities.compile(text, flags);

        setInitialSeekTerm(new BytesRef(""));
    }

    @Override
    protected AcceptStatus accept(BytesRef term) {
        return regexImpl.match(term) ? AcceptStatus.YES : AcceptStatus.NO;
    }
}
