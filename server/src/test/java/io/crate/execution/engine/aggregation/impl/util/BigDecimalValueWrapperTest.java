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

import org.junit.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

public class BigDecimalValueWrapperTest {
    // --- max ---

    @Test
    public void testMaxWhenThisHasNoValueReturnsOther() {
        var wrapper = new BigDecimalValueWrapper(null);
        var other = new BigDecimalValueWrapper(new BigDecimal("1"));
        assertThat(wrapper.max(other)).isSameAs(other);
    }

    @Test
    public void testMaxWhenOtherHasNoValueReturnsThis() {
        var wrapper = new BigDecimalValueWrapper(new BigDecimal("1"));
        var other = new BigDecimalValueWrapper(null);
        assertThat(wrapper.max(other)).isSameAs(wrapper);
    }

    @Test
    public void testMaxWhenBothHaveNoValueReturnsOther() {
        var wrapper = new BigDecimalValueWrapper(null);
        var other = new BigDecimalValueWrapper(null);
        assertThat(wrapper.max(other)).isSameAs(other);
    }

    @Test
    public void testMaxWhenThisIsGreaterReturnsThis() {
        var wrapper = new BigDecimalValueWrapper(new BigDecimal("10"));
        var other = new BigDecimalValueWrapper(new BigDecimal("5"));
        assertThat(wrapper.max(other)).isSameAs(wrapper);
    }

    @Test
    public void testMaxWhenOtherIsGreaterReturnsOther() {
        var wrapper = new BigDecimalValueWrapper(new BigDecimal("5"));
        var other = new BigDecimalValueWrapper(new BigDecimal("10"));
        assertThat(wrapper.max(other)).isSameAs(other);
    }

    @Test
    public void testMaxWhenEqualReturnsThis() {
        var wrapper = new BigDecimalValueWrapper(new BigDecimal("5"));
        var other = new BigDecimalValueWrapper(new BigDecimal("5"));
        assertThat(wrapper.max(other)).isSameAs(wrapper);
    }

    @Test
    public void testMaxWithNegativeNumbers() {
        var wrapper = new BigDecimalValueWrapper(new BigDecimal("-1"));
        var other = new BigDecimalValueWrapper(new BigDecimal("-2"));
        assertThat(wrapper.max(other)).isSameAs(wrapper);
    }

    @Test
    public void testMaxWithDifferentScales() {
        var wrapper = new BigDecimalValueWrapper(new BigDecimal("1.00"));
        var other = new BigDecimalValueWrapper(new BigDecimal("1.0"));
        assertThat(wrapper.max(other)).isSameAs(wrapper);
    }

    // --- min ---

    @Test
    public void testMinWhenThisHasNoValueReturnsOther() {
        var wrapper = new BigDecimalValueWrapper(null);
        var other = new BigDecimalValueWrapper(new BigDecimal("1"));
        assertThat(wrapper.min(other)).isSameAs(other);
    }

    @Test
    public void testMinWhenOtherHasNoValueReturnsThis() {
        var wrapper = new BigDecimalValueWrapper(new BigDecimal("1"));
        var other = new BigDecimalValueWrapper(null);
        assertThat(wrapper.min(other)).isSameAs(wrapper);
    }

    @Test
    public void testMinWhenBothHaveNoValueReturnsOther() {
        var wrapper = new BigDecimalValueWrapper(null);
        var other = new BigDecimalValueWrapper(null);
        assertThat(wrapper.min(other)).isSameAs(other);
    }

    @Test
    public void testMinWhenThisIsSmallerReturnsThis() {
        var wrapper = new BigDecimalValueWrapper(new BigDecimal("5"));
        var other = new BigDecimalValueWrapper(new BigDecimal("10"));
        assertThat(wrapper.min(other)).isSameAs(wrapper);
    }

    @Test
    public void testMinWhenOtherIsSmallerReturnsOther() {
        var wrapper = new BigDecimalValueWrapper(new BigDecimal("10"));
        var other = new BigDecimalValueWrapper(new BigDecimal("5"));
        assertThat(wrapper.min(other)).isSameAs(other);
    }

    @Test
    public void testMinWhenEqualReturnsThis() {
        var wrapper = new BigDecimalValueWrapper(new BigDecimal("5"));
        var other = new BigDecimalValueWrapper(new BigDecimal("5"));
        // <= 0 branch is hit -> returns this (opposite tie-breaking from max)
        assertThat(wrapper.min(other)).isSameAs(wrapper);
    }

    @Test
    public void testMinWithNegativeNumbers() {
        var wrapper = new BigDecimalValueWrapper(new BigDecimal("-2"));
        var other = new BigDecimalValueWrapper(new BigDecimal("-1"));
        assertThat(wrapper.min(other)).isSameAs(wrapper);
    }

    @Test
    public void testMinWithDifferentScales() {
        var wrapper = new BigDecimalValueWrapper(new BigDecimal("1.00"));
        var other = new BigDecimalValueWrapper(new BigDecimal("1.0"));
        // 1.00.compareTo(1.0) == 0, so <= 0 -> returns this
        assertThat(wrapper.min(other)).isSameAs(wrapper);
    }
}
