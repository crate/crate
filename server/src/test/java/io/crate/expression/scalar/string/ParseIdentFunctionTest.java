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
package io.crate.expression.scalar.string;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.Test;

import io.crate.exceptions.UnsupportedFunctionException;
import io.crate.expression.scalar.ScalarTestCase;

public class ParseIdentFunctionTest extends ScalarTestCase {

    @Test
    public void testZeroArguments() {
        assertThatThrownBy(() -> assertEvaluateNull("parse_ident()"))
            .isExactlyInstanceOf(UnsupportedFunctionException.class)
            .hasMessage("Invalid arguments in: parse_ident(). Valid types: (text)");
    }

    @Test
    public void testNullInputReturnsNull() {
        assertEvaluateNull("parse_ident(null)");
    }

    @Test
    public void testParsesUnqualifiedName() {
        assertEvaluate("parse_ident('customers')", List.of("customers"));
    }

    @Test
    public void testParsesQualifiedName() {
        assertEvaluate("parse_ident('doc.customers')", List.of("doc", "customers"));
    }

    @Test
    public void testParsesQuotedQualifiedName() {
        assertEvaluate("parse_ident('\"Doc\".\"Customers\"')", List.of("Doc", "Customers"));
    }

    @Test
    public void testInvalidSyntaxThrows() {
        assertThatThrownBy(() -> assertEvaluate("parse_ident('doc..customers')", List.of()))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("invalid name syntax:");
    }
}