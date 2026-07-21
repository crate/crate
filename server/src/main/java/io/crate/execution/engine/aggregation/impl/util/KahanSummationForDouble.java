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

package io.crate.execution.engine.aggregation.impl.util;

public class KahanSummationForDouble {

    private double error;

    public double sum(double sum, double value) {
        var correctedValue = value - error;
        var newSum = sum + correctedValue;
        var newError = (newSum - sum) - correctedValue;
        // Guard against a non-finite error. When the running sum overflows to
        // (+/-)Infinity, or a value/sum is NaN, `newError` becomes Infinity or NaN
        // (e.g. Infinity - Infinity => NaN). A non-finite error is meaningless and,
        // if kept, it affects every subsequent addition made, as a single instance
        // is reused across all GROUP BY keys.
        error = Double.isFinite(newError) ? newError : 0.0d;
        return newSum;
    }
}
