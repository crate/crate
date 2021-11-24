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

package io.crate.auth;

import io.crate.action.sql.SessionContext;
import io.crate.analyze.AnalyzedStatement;
import io.crate.analyze.validator.StatementValidator;
import io.crate.exceptions.MissingPrivilegeException;
import io.crate.user.UserLookup;

public interface AccessControl extends StatementValidator {

    AccessControl DISABLED = new AccessControl() {
        @Override
        public void ensureMayExecute(AnalyzedStatement statement, UserLookup userLookup, SessionContext sessionContext) {

        }

        @Override
        public void ensureMaySee(Throwable t) throws MissingPrivilegeException {

        }
    };

    void ensureMaySee(Throwable t) throws MissingPrivilegeException;
}
