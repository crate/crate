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

import io.crate.common.annotations.VisibleForTesting;
import io.crate.sql.parser.ParsingException;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.parser.antlr.v4.SqlBaseLexer;
import io.crate.sql.tree.QualifiedNameReference;

public class Identifiers {

    @VisibleForTesting
    static final Pattern IDENTIFIER = Pattern.compile("(^[a-z_]+[a-z0-9_]*)"
                                                      // can contain uppercase letters as long as quoted
                                                      + "|(^\"([a-z_]+[a-z0-9_]*)?[A-Z][a-zA-Z0-9_]*\")");
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
     * Identifiers can either be quoted or unquoted. Unquoted idents are quoted and escaped while quoted idents are
     * returned as-is.
     */
    public static String quote(String identifier) {
        return isQuoted(identifier) ? identifier : "\"" + escape(identifier) + "\"";
    }

    public static String unquote(String identifier) {
        return isQuoted(identifier) ?
            identifier.substring(1, identifier.length() - 1).replace("\"\"", "\"") :
            identifier;
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

    /**
     * Similar to {@link Identifiers#quoteIfNeeded}
     */
    public static String maybeQuoteExpression(String expression) {
        int length = expression.length();
        if (length == 0) {
            return "\"\"";
        }
        if (isKeyWord(expression)) {
            return '"' + expression + '"';
        }
        StringBuilder sb = new StringBuilder();
        boolean addQuotes = false;
        int subscriptStartPos = -1;
        for (int i = 0; i < length; i++) {
            char c = expression.charAt(i);
            if (c == '"') {
                sb.append('"');
            }
            sb.append(c);
            if (subscriptStartPos == -1) {
                if (c == '[' && i + 1 < length && expression.charAt(i + 1) == '\'') {
                    subscriptStartPos = i;
                } else {
                    addQuotes = addQuotes || charIsOutsideSafeRange(i, c);
                }
            }
        }
        if (addQuotes) {
            sb.insert(0, '"');
            if (subscriptStartPos == -1) {
                sb.append('"');
            } else {
                sb.insert(subscriptStartPos + 1, '"');
            }
        }
        return sb.toString();
    }

    private static boolean charIsOutsideSafeRange(int i, char c) {
        if (i == 0) {
            return c != '_' && (c < 'a' || c > 'z');
        }
        return c != '_' && (c < 'a' || c > 'z') && (c < '0' || c > '9');
    }

    private static boolean quotesRequired(String identifier) {
        return isKeyWord(identifier) || !IDENTIFIER.matcher(identifier).matches();
    }

    public static boolean isKeyWord(String identifier) {
        return RESERVED_KEYWORDS.contains(identifier.toUpperCase(Locale.ENGLISH));
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

            Matcher matcher = IDENTIFIER.matcher(literal.toLowerCase(Locale.ENGLISH));
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

    private static boolean isQuoted(String identifier) {
        return identifier.length() >= 2 &&
               identifier.charAt(0) == '"' && identifier.charAt(identifier.length() - 1) == '"';
    }
}
