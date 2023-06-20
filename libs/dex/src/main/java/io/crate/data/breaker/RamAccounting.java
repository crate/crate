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

package io.crate.data.breaker;

/**
 * Accounts for RAM usage by counting allocated bytes.
 * Throws a CircuitBreakingException to avoid OutOfMemoryErrors,
 * in case it thinks that too many bytes have been allocated.
 */
public interface RamAccounting {

    RamAccounting NO_ACCOUNTING = new RamAccounting() {
        @Override
        public void addBytes(long bytes) {
        }

        @Override
        public long totalBytes() {
            return 0;
        }

        @Override
        public void release() {
        }

        @Override
        public void close() {
        }
    };

    /**
     * Accounts for the supplied number of bytes
     * @throws CircuitBreakingException if too many bytes have been added.
     */
    void addBytes(long bytes);

    long totalBytes();

    /**
     * Stops accounting for previously accounted rows.
     */
    void release();

    /**
     * Closes this RamAccounting and prevents further use.
     */
    void close();

}
