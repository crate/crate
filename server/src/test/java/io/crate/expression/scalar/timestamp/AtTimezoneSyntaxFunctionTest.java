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

package io.crate.expression.scalar.timestamp;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.FunctionCall;
import io.crate.sql.tree.QualifiedName;
import io.crate.sql.tree.StringLiteral;

public class AtTimezoneSyntaxFunctionTest {

    @Test
    public void test_at_time_zone_is_parsed_as_timezone_function_call() {
        FunctionCall func = (FunctionCall) SqlParser.createExpression("'1978-02-28T10:00:00+01:00' AT TIME ZONE 'Europe/Madrid'");
        assertThat(QualifiedName.of("timezone")).isEqualTo(func.getName());
        assertThat(2).isEqualTo(func.getArguments().size());
        assertThat(StringLiteral.fromObject("Europe/Madrid")).isEqualTo(func.getArguments().get(0));
        assertThat(StringLiteral.fromObject("1978-02-28T10:00:00+01:00")).isEqualTo(func.getArguments().get(1));
    }
}
