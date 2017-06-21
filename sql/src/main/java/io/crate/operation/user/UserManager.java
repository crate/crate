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

package io.crate.operation.user;

import io.crate.action.sql.SessionContext;
import io.crate.analyze.AnalyzedStatement;
import io.crate.exceptions.UnauthorizedException;

import java.util.concurrent.CompletableFuture;

/**
 * responsible for creating and deleting users
 */
public interface UserManager extends UserLookup {

    /**
     * creates a user
     *
     * @param userName name of the user to create
     * @return a future which returns the number of rows when the User is created
     */
    CompletableFuture<Long> createUser(String userName);

    /**
     * deletes a user
     * @param userName name of the user to drop
     * @return a future which returns the number of rows when the User is dropped
     */
    CompletableFuture<Long> dropUser(String userName, boolean ifExists);

    /**
     * checks if user is allowed to execute statement
     * @param analysis          analysed statement
     * @param sessionContext    current session context
     * @throws UnauthorizedException if the user is not authorized to perform the statement
     */
    void ensureAuthorized(AnalyzedStatement analysis, SessionContext sessionContext);
}
