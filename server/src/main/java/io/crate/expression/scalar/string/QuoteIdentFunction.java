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

package io.crate.expression.scalar.string;

import static io.crate.sql.Identifiers.isKeyWord;

import org.jetbrains.annotations.VisibleForTesting;
import io.crate.expression.scalar.UnaryScalar;
import io.crate.metadata.FunctionName;
import io.crate.metadata.Functions;
import io.crate.metadata.Scalar;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;
import io.crate.sql.Identifiers;
import io.crate.types.DataTypes;


public final class QuoteIdentFunction {

    private static final FunctionName FQNAME = new FunctionName(PgCatalogSchemaInfo.NAME, "quote_ident");

    public static void register(Functions.Builder module) {
        module.add(
            Signature.scalar(
                    FQNAME,
                    DataTypes.STRING.getTypeSignature(),
                    DataTypes.STRING.getTypeSignature()
                ).withFeature(Scalar.Feature.DETERMINISTIC)
                .withFeature(Scalar.Feature.NULLABLE),
            (signature, boundSignature) ->
                new UnaryScalar<>(
                    signature,
                    boundSignature,
                    DataTypes.STRING,
                    QuoteIdentFunction::maybeQuoteExpression
                )
        );
    }

    /**
     * Similar to {@link Identifiers#quoteIfNeeded}.
     * The main difference is that for subscript expressions, this method will quote the base(root) columns only
     * when it's needed.
     */
    @VisibleForTesting
    static String maybeQuoteExpression(String expression) {
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
}
