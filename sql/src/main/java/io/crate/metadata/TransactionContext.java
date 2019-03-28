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

import io.crate.metadata.settings.SessionTransportableInfo;
import org.joda.time.DateTimeUtils;

public interface TransactionContext {

    static TransactionContext of(SessionTransportableInfo sessionInfo) {
        return new StaticTransactionContext(sessionInfo);
    }

    long currentTimeMillis();

    String userName();

    String currentSchema();

    SearchPath searchPath();

    SessionTransportableInfo sessionTransportableInfo();

    class StaticTransactionContext implements TransactionContext {

        private final SessionTransportableInfo sessionInfo;
        private Long currentTimeMillis;

        StaticTransactionContext(SessionTransportableInfo sessionInfo) {
            this.sessionInfo = sessionInfo;
        }

        @Override
        public long currentTimeMillis() {
            if (currentTimeMillis == null) {
                currentTimeMillis = DateTimeUtils.currentTimeMillis();
            }
            return currentTimeMillis;
        }

        @Override
        public String userName() {
            return sessionInfo.userName();
        }

        @Override
        public String currentSchema() {
            return sessionInfo.currentSchema();
        }

        @Override
        public SearchPath searchPath() {
            return sessionInfo.searchPath();
        }

        @Override
        public SessionTransportableInfo sessionTransportableInfo() {
            return sessionInfo;
        }
    }
}
