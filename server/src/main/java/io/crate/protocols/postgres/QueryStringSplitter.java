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

package io.crate.protocols.postgres;

import java.util.ArrayList;
import java.util.List;

import static io.crate.protocols.postgres.QueryStringSplitter.CommentType.LINE;
import static io.crate.protocols.postgres.QueryStringSplitter.CommentType.MULTI_LINE;
import static io.crate.protocols.postgres.QueryStringSplitter.CommentType.NO;
import static io.crate.protocols.postgres.QueryStringSplitter.QuoteType.NONE;
import static io.crate.protocols.postgres.QueryStringSplitter.QuoteType.SINGLE;
import static io.crate.protocols.postgres.QueryStringSplitter.QuoteType.DOUBLE;

/**
 * Splits a query string by semicolon into multiple statements.
 */
class QueryStringSplitter {

    enum CommentType {
        NO,
        LINE,
        MULTI_LINE
    }

    enum QuoteType {
        NONE,
        SINGLE,
        DOUBLE
    }

    public static List<String> splitQuery(String query) {
        final List<String> queries = new ArrayList<>(2);

        CommentType commentType = NO;
        QuoteType quoteType = NONE;
        boolean restoreSingleQuoteIfEscaped = false;

        char[] chars = query.toCharArray();

        boolean isCurrentlyEmpty = true;
        int offset = 0;
        char lastChar = ' ';
        for (int i = 0; i < chars.length; i++) {
            char aChar = chars[i];
            if (isCurrentlyEmpty && !Character.isWhitespace(aChar)) {
                isCurrentlyEmpty = false;
            }
            switch (aChar) {
                case '\'':
                    if (commentType == NO && quoteType != DOUBLE) {
                        if (lastChar == '\'') {
                            // Escaping of ' via '', e.g.: 'hello ''Joe''!'
                            // When we set single quotes state to NONE,
                            // but we find out this was due to an escaped single quote (''),
                            // we restore the previous single quote state here.
                            quoteType = restoreSingleQuoteIfEscaped ? SINGLE : NONE;
                            restoreSingleQuoteIfEscaped = false;
                        } else {
                            restoreSingleQuoteIfEscaped = quoteType == SINGLE;
                            quoteType = quoteType == SINGLE ? NONE : SINGLE;
                        }
                    }
                    break;
                case '"':
                    if (commentType == NO && quoteType != SINGLE) {
                        quoteType = quoteType == DOUBLE ? NONE : DOUBLE;
                    }
                    break;
                case '-':
                    if (commentType == NO && quoteType == NONE && lastChar == '-') {
                        commentType = LINE;
                    }
                    break;
                case '*':
                    if (commentType == NO && quoteType == NONE && lastChar == '/') {
                        commentType = MULTI_LINE;
                    }
                    break;
                case '/':
                    if (commentType == MULTI_LINE && lastChar == '*') {
                        commentType = NO;
                        offset = i + 1;
                    }
                    break;
                case '\n':
                    if (commentType == LINE) {
                        commentType = NO;
                        offset = i + 1;
                    }
                    break;
                case ';':
                    if (commentType == NO && quoteType == NONE) {
                        queries.add(new String(chars, offset, i - offset + 1));
                        offset = i + 1;
                        isCurrentlyEmpty = true;
                    }
                    break;

                default:
            }
            lastChar = aChar;
        }
        // statement might not be terminated by semicolon
        if (!isCurrentlyEmpty && offset < chars.length && commentType == NO) {
            queries.add(new String(chars, offset, chars.length - offset));
        }
        if (queries.isEmpty()) {
            queries.add("");
        }

        return queries;
    }
}
