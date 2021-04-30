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

package io.crate.breaker;

import io.crate.expression.reference.sys.operation.OperationContextLog;
import org.apache.lucene.util.RamUsageEstimator;

import javax.annotation.Nullable;

public class OperationContextLogSizeEstimator extends SizeEstimator<OperationContextLog> {
    @Override
    public long estimateSize(@Nullable OperationContextLog value) {
        long size = 0L;

        // OperationContextLog
        size += 32L; // 24 bytes (ref+headers) + 8 bytes (ended)
        size += value.errorMessage() == null ? 0 : value.errorMessage().length();  // error message

        // OperationContext
        size += 60L; // 24 bytes (headers) + 4 bytes (id) + 16 bytes (uuid) + 8 bytes (started) + 8 bytes (usedBytes)
        size += value.name().length();

        return RamUsageEstimator.alignObjectSize(size);
    }
}
