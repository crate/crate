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

import java.math.BigDecimal;

public final class BigDecimalValueWrapper implements NumericValueHolder {

    private BigDecimal value;

    public BigDecimalValueWrapper(BigDecimal value) {
        this.value = value;
    }

    @Override
    public BigDecimal value() {
        return value;
    }

    @Override
    public void setValue(BigDecimal value) {
        this.value = value;
    }

    /**
     * Analogue to {@link java.math.BigDecimal#max(BigDecimal)}, with the addition
     * that if one of the wrappers has no value or is null, the other is returned.
     */
    public BigDecimalValueWrapper max(BigDecimalValueWrapper other) {
        if (value() == null) {
            return other;
        }
        if (other == null || other.value() == null) {
            return this;
        }
        if (value().compareTo(other.value()) >= 0) {
            return this;
        } else {
            return other;
        }
    }

    /**
     * Analogue to {@link java.math.BigDecimal#min(BigDecimal)}, with the addition
     * that if one of the wrappers has no value or is null, the other is returned.
     */
    public BigDecimalValueWrapper min(BigDecimalValueWrapper other) {
        if (value() == null) {
            return other;
        }
        if (other == null || other.value() == null) {
            return this;
        }
        if (value().compareTo(other.value()) <= 0) {
            return this;
        } else {
            return other;
        }
    }
}
