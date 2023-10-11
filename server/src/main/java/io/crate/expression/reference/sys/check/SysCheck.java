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

package io.crate.expression.reference.sys.check;


import io.crate.common.annotations.ThreadSafe;
import java.util.concurrent.CompletableFuture;

@ThreadSafe
public interface SysCheck {

    enum Severity {
        LOW(1),
        MEDIUM(2),
        HIGH(3);

        private final int value;

        Severity(int value) {
            this.value = value;
        }

        public int value() {
            return value;
        }
    }

    /**
     * Returns the unique id of the check.
     *
     * We have a unique link per check (https://cr8.is/d-cluster-check-ID) which points to some documentation for users,
     * so that they can get an idea on what to do about a warning or error.
     *
     * Currently we can't change the links to point something else, which is why new checks must use a new ID and not re-use old IDs
     * (sometimes we delete sys checks when they're no longer appropriate)
     */
    int id();

    /**
     * Returns the message description relevant for the check.
     */
    String description();

    /**
     * Returns the level of {@link Severity} for the check.
     */
    Severity severity();

    /**
     * The validator method, used for e.g. validation of a setting.
     *
     * @return true if validation is passed.
     */
    boolean isValid();

    /**
     * Start the checks in an async manner and returns a future
     * which the caller can use as a callback.
     */
    CompletableFuture<?> computeResult();
}
