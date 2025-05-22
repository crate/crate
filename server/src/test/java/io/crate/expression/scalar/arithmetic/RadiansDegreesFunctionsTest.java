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

package io.crate.expression.scalar.arithmetic;

import static io.crate.testing.Asserts.isLiteral;

import java.math.BigDecimal;

import org.junit.Test;

import io.crate.expression.scalar.ScalarTestCase;

public class RadiansDegreesFunctionsTest extends ScalarTestCase {

    @Test
    public void test_radians_scalar() {
        assertNormalize("radians(45.0)", isLiteral(0.7853981633974483));
        assertNormalize("radians(12.345::NUMERIC)",
            isLiteral(new BigDecimal("0.2154608961586999862712296253699192")));
    }

    @Test
    public void test_degrees_scalar() {
        assertNormalize("degrees(0.5)", isLiteral(28.64788975654116));
        assertNormalize("degrees(12.345::NUMERIC)",
            isLiteral(new BigDecimal("707.3163980890012512240732211801283")));
    }
}
