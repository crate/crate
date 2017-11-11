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

package io.crate.user;

import com.google.common.annotations.VisibleForTesting;
import io.crate.analyze.CreateUserAnalyzedStatement;
import io.crate.analyze.SymbolEvaluator;
import io.crate.analyze.symbol.Symbol;
import io.crate.data.Row;
import io.crate.metadata.Functions;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.settings.SecureString;

import java.security.GeneralSecurityException;
import java.util.Locale;
import java.util.Map;

public final class UserActions {

    private UserActions() {
    }

    public static SecureHash generateSecureHash(CreateUserAnalyzedStatement statement,
                                                Row parameters,
                                                Functions functions) throws GeneralSecurityException, IllegalArgumentException {
        try (SecureString pw = getUserPasswordProperty(statement.properties(), parameters, functions)) {
            SecureHash secureHash = null;
            if (pw != null) {
                if (pw.length() == 0) {
                    throw new IllegalArgumentException("Password must not be empty");
                }
                secureHash = SecureHash.of(pw);
            }
            return secureHash;
        }
    }

    @VisibleForTesting
    static SecureString getUserPasswordProperty(Map<String, Symbol> properties,
                                                Row parameters,
                                                Functions functions) throws IllegalArgumentException {
        final String PASSWORD_PROPERTY = "password";
        if (properties != null) {
            for (String key : properties.keySet()) {
                if (PASSWORD_PROPERTY.equals(key)) {
                    String value = BytesRefs.toString(SymbolEvaluator.evaluate(functions, properties.get(key), parameters));
                    return new SecureString(value.toCharArray());
                } else {
                    throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                        "\"%s\" is not a valid setting for CREATE USER", key));
                }
            }
        }
        return null;
    }
}
