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

package io.crate.execution.engine.aggregation.statistics;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;

import org.elasticsearch.common.io.stream.StreamInput;

import ch.obermuhlner.math.big.BigDecimalMath;

public class NumericStandardDeviationSamp extends NumericVariance {

    public NumericStandardDeviationSamp() {
    }

    public NumericStandardDeviationSamp(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public BigDecimal result() {
        long count = super.count();
        if (count <= 1) {
            return null;
        }
        return BigDecimalMath.sqrt(
            super.result().multiply(
                BigDecimal.valueOf(count)).divide(BigDecimal.valueOf(count - 1),
                MathContext.DECIMAL128),
            MathContext.DECIMAL128);
    }
}
