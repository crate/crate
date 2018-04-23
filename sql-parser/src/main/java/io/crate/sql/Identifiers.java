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

package io.crate.sql;

import io.crate.sql.parser.SqlParser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Identifiers {

    private static final Pattern ESCAPE_REPLACE_RE = Pattern.compile("\"", Pattern.LITERAL);
    private static final String ESCAPE_REPLACEMENT = Matcher.quoteReplacement("\"\"");

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
        if (areQuotesRequired(identifier)) {
            return quote(identifier);
        }
        return identifier;
    }

    private static boolean areQuotesRequired(String identifier) {
        for (int i = 0; i < identifier.length(); i++) {
            int cp = identifier.codePointAt(i);
            if (cp == '"' || cp == '[' || cp == ']' || cp == '(' || cp == ')' || Character.isUpperCase(cp)) {
                return true;
            }
        }
        return isKeyWord(identifier);
    }

    public static boolean isKeyWord(String identifier) {
        if (identifier.length() < 1) {
            return false;
        }
        // TODO: this is causing gazillion allocations and creates expensive exceptions. FIX THIS
        try {
            SqlParser.createIdentifier(identifier);
            return false;
        } catch (Throwable e) {
            return true;
        }
    }

    /**
     * escape the given identifier
     * i.e. replace " with ""
     */
    public static String escape(String identifier) {
        return ESCAPE_REPLACE_RE.matcher(identifier).replaceAll(ESCAPE_REPLACEMENT);
    }
}
