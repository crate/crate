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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;

/**
 * Subclass of FilteredTermEnum for enumerating all terms that match the
 * specified regular expression term using the specified regular expression
 * implementation.
 * <p>
 * Term enumerations are always ordered by Term.compareTo().  Each term in
 * the enumeration is greater than all that precede it.
 */
class RegexTermsEnum extends FilteredTermsEnum {

    private final Matcher matcher;
    private final CharsRefBuilder utf16 = new CharsRefBuilder();

    RegexTermsEnum(TermsEnum tenum, Term term, int flags) {
        super(tenum);
        Pattern pattern = Pattern.compile(term.text(), flags);
        this.matcher = pattern.matcher(utf16.get());
        setInitialSeekTerm(new BytesRef(""));
    }

    @Override
    protected AcceptStatus accept(BytesRef term) {
        utf16.copyUTF8Bytes(term);
        return matcher.reset().matches() ? AcceptStatus.YES : AcceptStatus.NO;
    }
}
