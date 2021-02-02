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

package io.crate.exceptions;

import io.crate.user.Privilege;

import java.util.Locale;

public class MissingPrivilegeException extends UnauthorizedException {

    private static final String MESSAGE_TMPL = "Missing '%s' privilege for user '%s'";

    public MissingPrivilegeException(String userName, Privilege.Type type) {
        super(String.format(Locale.ENGLISH, MESSAGE_TMPL, type, userName));
    }

    public MissingPrivilegeException(String userName) {
        super(String.format(Locale.ENGLISH, "Missing privilege for user '%s'", userName));
    }

}
