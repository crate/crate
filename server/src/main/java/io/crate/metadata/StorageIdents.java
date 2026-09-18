/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.metadata;

import java.util.function.LongFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jspecify.annotations.Nullable;

/**
 * Utilities to map the storage identifiers Lucene knows about back to the column names used in SQL.
 *
 * <p>
 * Lucene only knows the {@link Reference#storageIdent()} of a column, which is the {@code oid} for
 * tables created with CrateDB 5.5 or newer. Any text originating from Lucene, like an exception
 * message or the query description shown by {@code EXPLAIN ANALYZE}, therefore contains the oid
 * instead of the column name, which is an internal detail users cannot make sense of.
 * </p>
 */
public final class StorageIdents {

    /**
     * Matches the storage identifiers Lucene names in its messages: digits which are quoted
     * and introduced by {@code field=} or {@code DocValuesField}, e.g. {@code field="5615"},
     * or which are followed by a colon, e.g. {@code 1:[-9223372036854775808 TO 1741790715]}.
     */
    private static final Pattern FIELD_NAME = Pattern.compile("(?<=field=|DocValuesField )\"(\\d+)\"|\\b(\\d+):");

    private StorageIdents() {}

    /**
     * Replaces the storage identifiers within {@code text} with the name of the column they belong to.
     *
     * <p>
     * Digits which don't resolve to a column are left untouched, to not mangle values or limits
     * which are part of the same text.
     * </p>
     *
     * @param resolveOid must return {@code null} if the oid doesn't belong to a column
     */
    @Nullable
    public static String replaceOids(@Nullable String text, LongFunction<ColumnIdent> resolveOid) {
        if (text == null || text.isEmpty()) {
            return text;
        }
        Matcher matcher = FIELD_NAME.matcher(text);
        StringBuilder result = null;
        int copiedUpTo = 0;
        while (matcher.find()) {
            boolean quoted = matcher.group(1) != null;
            ColumnIdent column = resolve(quoted ? matcher.group(1) : matcher.group(2), resolveOid);
            if (column == null) {
                continue;
            }
            if (result == null) {
                result = new StringBuilder(text.length());
            }
            result.append(text, copiedUpTo, matcher.start());
            if (quoted) {
                result.append('"').append(column.sqlFqn()).append('"');
            } else {
                result.append(column.sqlFqn()).append(':');
            }
            copiedUpTo = matcher.end();
        }
        if (result == null) {
            return text;
        }
        return result.append(text, copiedUpTo, text.length()).toString();
    }

    /**
     * Returns a copy of {@code e} with the storage identifiers of its message replaced by the
     * column names, or {@code e} itself if there is nothing to replace.
     *
     * <p>
     * Only {@link IllegalArgumentException} is re-created, as that is the type Lucene uses to
     * reject invalid field values. Exceptions of any other type are returned as is, to not change
     * how they are handled further up the stack.
     * </p>
     */
    public static Exception replaceOids(Exception e, DocTableInfo table) {
        if (e instanceof IllegalArgumentException == false) {
            return e;
        }
        String message = e.getMessage();
        if (message == null) {
            return e;
        }
        String newMessage = replaceOids(message, oid -> {
            Reference ref = table.getReference(oid);
            return ref == null ? null : ref.column();
        });
        if (message.equals(newMessage)) {
            return e;
        }
        return new IllegalArgumentException(newMessage, e);
    }

    @Nullable
    private static ColumnIdent resolve(String digits, LongFunction<ColumnIdent> resolveOid) {
        try {
            return resolveOid.apply(Long.parseLong(digits));
        } catch (NumberFormatException e) {
            // Too many digits to be an oid
            return null;
        }
    }
}
