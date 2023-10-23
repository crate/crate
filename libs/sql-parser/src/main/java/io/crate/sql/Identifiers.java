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

package io.crate.sql;

import java.util.Collection;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.Vocabulary;

import io.crate.sql.parser.ParsingException;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.parser.antlr.SqlBaseLexer;
import io.crate.sql.tree.QualifiedNameReference;

public class Identifiers {

    private static final Pattern IDENTIFIER = Pattern.compile("(^[a-zA-Z_]+[a-zA-Z0-9_]*)");
    private static final Pattern ESCAPE_REPLACE_RE = Pattern.compile("\"", Pattern.LITERAL);
    private static final String ESCAPE_REPLACEMENT = Matcher.quoteReplacement("\"\"");

    public static class Keyword {
        private final String word;
        private final boolean reserved;

        public Keyword(String word, boolean reserved) {
            this.word = word;
            this.reserved = reserved;
        }

        public String getWord() {
            return word;
        }

        public boolean isReserved() {
            return reserved;
        }
    }

    public static final Collection<Keyword> KEYWORDS = identifierCandidates();

    static final Set<String> RESERVED_KEYWORDS = KEYWORDS.stream()
        .filter(Keyword::isReserved)
        .map(Keyword::getWord)
        .collect(Collectors.toSet());

    /**
     * quote and escape the given identifier
     */
    public static String quote(String identifier) {
        return "\"" + escape(identifier) + "\"";
    }

    /**
     * quote and escape identifier only if it needs to be quoted to be a valid identifier
     * i.e. when it contain a double-quote, has upper case letters or is a SQL keyword
     */
    public static String quoteIfNeeded(String identifier) {
        if (quotesRequired(identifier)) {
            return quote(identifier);
        }
        return identifier;
    }

    private static boolean quotesRequired(String identifier) {
        return isKeyWord(identifier) || !quotesNotRequired(identifier);
    }

    public static boolean isKeyWord(String identifier) {
        return RESERVED_KEYWORDS.contains(identifier.toUpperCase(Locale.ENGLISH));
    }

    /**
     * Quotes are not required only if:
     *   1) the first char is a lower case or '_' and
     *   2) the remaining chars are lower cases, digits, or '_'.
     */
    private static boolean quotesNotRequired(String identifier) {
        assert identifier != null && !identifier.isEmpty() : "null or empty idents should not be possible";

        char c = identifier.charAt(0);
        if (c != '_' && !isLowerCase(c)) {
            return false;
        }
        for (int i = 1; i < identifier.length(); i++) {
            c = identifier.charAt(i);
            if (c != '_' && !isLowerCase(c) && !isDigit(c)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isDigit(char c) {
        return c >= '0' && c <= '9';
    }

    private static boolean isLowerCase(char c) {
        return c >= 'a' && c <= 'z';
    }

    private static boolean reserved(String expression) {
        try {
            return !(SqlParser.createExpression(expression) instanceof QualifiedNameReference);
        } catch (ParsingException ignored) {
            return true;
        }
    }

    private static Set<Keyword> identifierCandidates() {
        HashSet<Keyword> candidates = new HashSet<>();
        Vocabulary vocabulary = SqlBaseLexer.VOCABULARY;
        for (int i = 0; i < vocabulary.getMaxTokenType(); i++) {
            String literal = vocabulary.getLiteralName(i);
            if (literal == null) {
                continue;
            }
            literal = literal.replace("'", "");
            Matcher matcher = IDENTIFIER.matcher(literal);
            if (matcher.matches()) {
                candidates.add(new Keyword(literal, reserved(literal)));
            }
        }
        return candidates;
    }

    /**
     * escape the given identifier
     * i.e. replace " with ""
     */
    public static String escape(String identifier) {
        return ESCAPE_REPLACE_RE.matcher(identifier).replaceAll(ESCAPE_REPLACEMENT);
    }
}
