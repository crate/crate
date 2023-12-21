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

package io.crate.role.metadata;

import static io.crate.testing.Asserts.assertThat;

import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.common.settings.SecureString;
import org.jetbrains.annotations.Nullable;

import io.crate.role.Privilege;
import io.crate.role.Role;
import io.crate.role.SecureHash;

public final class RolesHelper {

    public static final Map<String, Role> SINGLE_USER_ONLY = Collections.singletonMap("Arthur", userOf("Arthur"));

    public static final Map<String, Role> DUMMY_USERS = Map.of(
        "Ford", userOf("Ford", getSecureHash("fords-password")),
        "Arthur", userOf("Arthur", getSecureHash("arthurs-password"))
    );

    public static final Map<String, Role> DUMMY_USERS_WITHOUT_PASSWORD = Map.of(
        "Ford", userOf("Ford"),
        "Arthur", userOf("Arthur")
    );

    public static final Map<String, Role> DUMMY_USERS_AND_ROLES = new HashMap<>();

    static {
        DUMMY_USERS_AND_ROLES.put("Ford", userOf("Ford", getSecureHash("fords-pwd")));
        DUMMY_USERS_AND_ROLES.put("John", userOf("John", getSecureHash("johns-pwd")));
        DUMMY_USERS_AND_ROLES.put("DummyRole", roleOf("DummyRole"));
    }

    public static final Map<String, Role> DUMMY_USERS_AND_ROLES_WITHOUT_PASSWORD = new HashMap<>();

    static {
        DUMMY_USERS_AND_ROLES_WITHOUT_PASSWORD.put("Ford", userOf("Ford"));
        DUMMY_USERS_AND_ROLES_WITHOUT_PASSWORD.put("John", userOf("John"));
        DUMMY_USERS_AND_ROLES_WITHOUT_PASSWORD.put("DummyRole", roleOf("DummyRole"));
    }

    public static UsersMetadata usersMetadataOf(Map<String, Role> users) {
        Map<String, SecureHash> map = new HashMap<>(users.size());
        for (var user : users.entrySet()) {
            if (user.getValue().isUser())
                map.put(user.getKey(), user.getValue().password());
        }
        return new UsersMetadata(Collections.unmodifiableMap(map));
    }

    public static SecureHash getSecureHash(String password) {
        SecureHash hash = null;
        try {
            hash = SecureHash.of(new SecureString(password.toCharArray()));
        } catch (GeneralSecurityException e) {
            // do nothing;
        }
        assertThat(hash).isNotNull();
        return hash;
    }

    public static Role userOf(String name) {
        return userOf(name, null);
    }

    public static Role userOf(String name, @Nullable SecureHash password) {
        return userOf(name, Set.of(), password);
    }

    public static Role userOf(String name, Set<Privilege> privileges, @Nullable SecureHash password) {
        return new Role(name, true, privileges, password, Set.of());
    }

    public static Role roleOf(String name) {
        return new Role(name, false, Set.of(), null, Set.of());
    }
}
