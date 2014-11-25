/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.unit.ByteSizeValue;

public class MemoryCircuitBreaker extends org.elasticsearch.common.breaker.MemoryCircuitBreaker {

    private final ESLogger logger;

    public MemoryCircuitBreaker(ByteSizeValue limit, double overheadConstant, ESLogger logger) {
        super(limit, overheadConstant, logger);
        this.logger = logger;
    }

    @Override
    public void circuitBreak(String label, long bytesNeeded) throws CircuitBreakingException {
        try {
            super.circuitBreak(label, bytesNeeded);
        } catch (CircuitBreakingException e) {
            final String message = "Too much HEAP memory used by [" + label + "], usage would be larger than limit of [" +
                    getLimit() + "/" + new ByteSizeValue(getLimit()) + "]";
            logger.debug(message);
            throw new CircuitBreakingException(message);
        }
    }
}
