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

package io.crate.executor.transport;

import com.google.common.annotations.VisibleForTesting;
import io.crate.analyze.CreateUserAnalyzedStatement;
import io.crate.analyze.expressions.ExpressionToStringVisitor;
import io.crate.data.Row;
import io.crate.sql.tree.GenericProperties;
import io.crate.user.SecureHash;
import org.elasticsearch.common.settings.SecureString;

import java.security.GeneralSecurityException;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

import static io.crate.concurrent.CompletableFutures.failedFuture;

public final class UserActions {

    private UserActions() {

    }

    public static CompletableFuture<Long> execute(BiFunction<String, SecureHash, CompletableFuture<Long>> userAction,
                                                  CreateUserAnalyzedStatement statement,
                                                  Row parameters) {
        try (SecureString pw = getUserPasswordProperty(statement.properties(), parameters)) {
            SecureHash secureHash = null;
            if (pw != null) {
                secureHash = SecureHash.of(pw);
            }
            return userAction.apply(statement.userName(), secureHash);
        } catch (GeneralSecurityException e) {
            return failedFuture(e);
        }
    }

    @VisibleForTesting
    static SecureString getUserPasswordProperty(GenericProperties properties, Row parameters) throws IllegalArgumentException {
        final String PASSWORD_PROPERTY = "password";
        if (properties != null) {
            for (String key : properties.keys()) {
                if (PASSWORD_PROPERTY.equals(key)) {
                    return new SecureString(ExpressionToStringVisitor.convert(
                        properties.get(PASSWORD_PROPERTY),
                        parameters).toCharArray());
                } else {
                    throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                        "\"%s\" is not a valid setting for CREATE USER", key));
                }
            }
        }
        return null;
    }
}
