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

package io.crate.user;

import io.crate.analyze.SymbolEvaluator;
import io.crate.common.annotations.VisibleForTesting;
import io.crate.data.Row;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.planner.operators.SubQueryResults;
import io.crate.sql.tree.GenericProperties;
import io.crate.types.DataTypes;
import org.elasticsearch.common.settings.SecureString;

import org.jetbrains.annotations.Nullable;
import java.security.GeneralSecurityException;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

public final class UserActions {

    private UserActions() {
    }

    @Nullable
    public static SecureHash generateSecureHash(GenericProperties<Symbol> userStmtProperties,
                                                Row parameters,
                                                TransactionContext txnCtx,
                                                NodeContext nodeCtx) throws GeneralSecurityException, IllegalArgumentException {
        try (SecureString pw = getUserPasswordProperty(userStmtProperties, parameters, txnCtx, nodeCtx)) {
            if (pw != null) {
                if (pw.isEmpty()) {
                    throw new IllegalArgumentException("Password must not be empty");
                }
                return SecureHash.of(pw);
            }
            return null;
        }
    }

    @VisibleForTesting
    @Nullable
    static SecureString getUserPasswordProperty(GenericProperties<Symbol> userStmtProperties,
                                                Row parameters,
                                                TransactionContext txnCtx,
                                                NodeContext nodeCtx) throws IllegalArgumentException {
        Function<? super Symbol, Object> eval = x -> SymbolEvaluator.evaluate(
            txnCtx,
            nodeCtx,
            x,
            parameters,
            SubQueryResults.EMPTY
        );
        Map<String, Object> properties = userStmtProperties.map(eval).properties();
        final String PASSWORD_PROPERTY = "password";
        for (var entry : properties.entrySet()) {
            if (PASSWORD_PROPERTY.equals(entry.getKey())) {
                String value = DataTypes.STRING.sanitizeValue(entry.getValue());
                if (value != null) {
                    return new SecureString(value.toCharArray());
                }
                // Password will be reset
                return null;
            } else {
                throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "\"%s\" is not a valid user property", entry.getKey()));
            }
        }
        return null;
    }
}
