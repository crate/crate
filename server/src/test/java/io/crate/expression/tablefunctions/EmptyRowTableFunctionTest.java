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

package io.crate.expression.tablefunctions;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.StreamSupport;

import org.junit.Test;

import io.crate.data.Row;


public class EmptyRowTableFunctionTest extends AbstractTableFunctionsTest {

    @Test
    public void testEmptyRowReturnsEmptyResult() {
        assertExecute("empty_row()", "\n");
    }

    @Test
    public void testEmptyRowReturnsOneRow() {
        Iterable<Row> result = execute("empty_row()");
        assertThat(StreamSupport.stream(result.spliterator(), false).count()).isEqualTo(1L);
    }
}
