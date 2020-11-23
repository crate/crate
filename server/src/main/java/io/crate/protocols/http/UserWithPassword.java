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

package io.crate.protocols.http;

import java.util.Objects;

import javax.annotation.Nullable;

import org.elasticsearch.common.settings.SecureString;

public final class UserWithPassword {

    private final String userName;
    private final SecureString password;

    public UserWithPassword(String userName, @Nullable SecureString password) {
        this.userName = userName;
        this.password = password;
    }

    public String userName() {
        return userName;
    }

    public SecureString password() {
        return password;
    }

    @Override
    public String toString() {
        return "UserWithPassword{" + userName + "}";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((password == null) ? 0 : password.hashCode());
        result = prime * result + userName.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        UserWithPassword other = (UserWithPassword) obj;
        return userName.equals(other.userName()) && Objects.equals(password, other.password);
    }
}
