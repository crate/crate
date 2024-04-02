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

package io.crate.exceptions;

import java.util.Collections;
import java.util.List;
import java.util.Locale;

public class RoleUnknownException extends RuntimeException implements ResourceUnknownException, UnscopedException {

    public RoleUnknownException(String roleName) {
        super(getMessage(Collections.singletonList(roleName)));
    }

    public RoleUnknownException(int oid) {
        super(String.format(Locale.ENGLISH, "Role with OID %d does not exist", oid));
    }

    public RoleUnknownException(List<String> roleNames) {
        super(getMessage(roleNames));
    }

    private static String getMessage(List<String> roleNames) {
        //noinspection PointlessBooleanExpression
        assert roleNames.isEmpty() == false : "At least one username must be provided";
        if (roleNames.size() == 1) {
            return String.format(Locale.ENGLISH, "Role '%s' does not exist", roleNames.get(0));
        }
        return String.format(Locale.ENGLISH, "Roles '%s' do not exist", String.join(", ", roleNames));
    }
}
