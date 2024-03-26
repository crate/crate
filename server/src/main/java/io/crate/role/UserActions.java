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

package io.crate.role;

import org.jetbrains.annotations.VisibleForTesting;
import io.crate.planner.node.ddl.CreateRolePlan;
import io.crate.types.DataTypes;
import org.elasticsearch.common.settings.SecureString;

import org.jetbrains.annotations.Nullable;
import java.security.GeneralSecurityException;
import java.util.Map;

public final class UserActions {

    private UserActions() {
    }

    @Nullable
    public static SecureHash generateSecureHash(Map<String, Object> properties) throws GeneralSecurityException, IllegalArgumentException {
        try (SecureString pw = getUserPasswordProperty(properties)) {
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
    static SecureString getUserPasswordProperty(Map<String, Object> properties) {
        String value = DataTypes.STRING.sanitizeValue(properties.get(CreateRolePlan.PASSWORD_PROPERTY_KEY));
        if (value != null) {
            return new SecureString(value.toCharArray());
        }
        return null;
    }
}
