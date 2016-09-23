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

package io.crate.metadata;

import org.joda.time.DateTimeUtils;

/**
 * StatementContext that can be used to keep state which is valid on the handler during a "statement lifecycle".
 */
public class StmtCtx {

    private Long currentTimeMillis = null;

    /**
     * @return current timestamp in ms. Subsequent calls will always return the same value. (Not thread-safe)
     */
    public long currentTimeMillis() {
        if (currentTimeMillis == null) {
            // no synchronization because StmtCtx is mostly used during single-threaded analysis phase
            currentTimeMillis = DateTimeUtils.currentTimeMillis();
        }
        return currentTimeMillis;
    }
}
