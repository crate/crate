/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.expression.scalar.regex;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.common.lucene.BytesRefs;

import javax.annotation.Nullable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexMatcher {

    private final Matcher matcher;
    private final CharsRef utf16 = new CharsRef(10);
    private final boolean globalFlag;

    public RegexMatcher(String regex, int flags, boolean globalFlag) {
        Pattern pattern = Pattern.compile(regex, flags);
        this.matcher = pattern.matcher(utf16);
        this.globalFlag = globalFlag;
    }

    public RegexMatcher(String regex, @Nullable BytesRef flags) {
        this(regex, parseFlags(BytesRefs.toString(flags)), isGlobal(BytesRefs.toString(flags)));
    }

    public RegexMatcher(String regex, @Nullable String flags) {
        this(regex, parseFlags(flags), isGlobal(flags));
    }

    public RegexMatcher(String regex) {
        this(regex, 0, false);
    }

    private static void utf8toUtf16(BytesRef bytes, CharsRef charsRef) {
        if (charsRef.chars.length < bytes.length) {
            charsRef.chars = new char[bytes.length];
        }
        charsRef.length = UnicodeUtil.UTF8toUTF16(bytes, charsRef.chars);
    }

    public boolean match(BytesRef term) {
        utf8toUtf16(term, utf16);
        return matcher.reset().find();
    }

    @Nullable
    public BytesRef[] groups() {
        try {
            if (matcher.groupCount() == 0) {
                return new BytesRef[]{new BytesRef(matcher.group())};
            }
            BytesRef[] groups = new BytesRef[matcher.groupCount()];
            // skip first group (the original string)
            for (int i = 1; i <= matcher.groupCount(); i++) {
                String group = matcher.group(i);
                if (group != null) {
                    groups[i - 1] = new BytesRef(group);
                } else {
                    groups[i - 1] = null;
                }
            }
            return groups;
        } catch (IllegalStateException e) {
            // no match -> no groups
        }
        return null;
    }

    public BytesRef replace(BytesRef term, String replacement) {
        utf8toUtf16(term, utf16);
        if (globalFlag) {
            return new BytesRef(matcher.replaceAll(replacement));
        } else {
            return new BytesRef(matcher.replaceFirst(replacement));
        }
    }

    public static int parseFlags(@Nullable String flagsString) {
        int flags = 0;
        if (flagsString == null) {
            return flags;
        }
        for (char flag : flagsString.toCharArray()) {
            switch (flag) {
                case 'i':
                    flags = flags | Pattern.CASE_INSENSITIVE;
                    break;
                case 'u':
                    flags = flags | Pattern.UNICODE_CASE;
                    break;
                case 'U':
                    flags = flags | Pattern.UNICODE_CHARACTER_CLASS;
                    break;
                case 's':
                    flags = flags | Pattern.DOTALL;
                    break;
                case 'm':
                    flags = flags | Pattern.MULTILINE;
                    break;
                case 'x':
                    flags = flags | Pattern.COMMENTS;
                    break;
                case 'd':
                    flags = flags | Pattern.UNIX_LINES;
                    break;
                case ' ':
                case 'g':
                    // handled in isGlobalFunction
                    break;
                default:
                    throw new IllegalArgumentException("The regular expression flag is unknown: " + flag);
            }
        }

        return flags;
    }

    public static boolean isGlobal(@Nullable String flags) {
        if (flags == null) {
            return false;
        }
        return flags.indexOf('g') != -1;
    }


    // PCRE features
    public static final String character_classes = "dDsSwW";
    public static final String boundary_matchers = "bBAGZz";
    public static final String embedded_flags = "idmsuxU";

    // recognize pcre escaped sequences anywhere inside the pattern
    public static final String escape_sequences_pattern = ".*\\\\[" + character_classes + boundary_matchers + "].*";

    // recognize pcre embedded flags at the beginning of the pattern
    public static final String embedded_flags_pattern = "^\\(\\?[" + embedded_flags + "]\\).*";

    // final precompiled java.util.regex.Pattern
    public static final Pattern pcre_pattern = Pattern.compile(escape_sequences_pattern + "|" + embedded_flags_pattern);

    /**
     * Determine whether regex pattern contains PCRE features, e.g.
     * - predefined character classes like \d, \D, \s, \S, \w, \W
     * - boundary matchers like \b, \B, \A, \G, \Z, \z
     * - embedded flag expressions like (?i), (?d), etc.
     *
     * @see java.util.regex.Pattern
     */
    public static boolean isPcrePattern(String pattern) {
        return pcre_pattern.matcher(pattern).matches();
    }
}
