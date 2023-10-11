/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.testing;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.assertj.core.api.AbstractAssert;

public final class SQLResponseAssert extends AbstractAssert<SQLResponseAssert, SQLResponse> {

    public SQLResponseAssert(SQLResponse actual) {
        super(actual, SQLResponseAssert.class);
    }

    public SQLResponseAssert hasRowCount(long expectedRowCount) {
        isNotNull();
        assertThat(actual.rowCount()).isEqualTo(expectedRowCount);
        return this;
    }

    /**
     * Like {@link #hasRows(String...)} but if a single row contains newlines, it is treated as multiple lines.
     **/
    public SQLResponseAssert hasLines(String ... lines) {
        String result = TestingHelpers.printedTable(actual.rows());
        String[] resultRows = result.split("\n");
        assertThat(resultRows).containsExactly(lines);
        return this;
    }

    /**
     * Assert that the response contains the given rows
     * <ul>
     * <li>Use {@link #hasLines(String...)} to treat newlines in a single row as separate rows.</li>
     * <li>Use {@link #hasRows(Object[]...)} for exact object matches instead of string formatting</li>
     * </ul>
     **/
    public SQLResponseAssert hasRows(String ... rows) {
        String[] resultRows = new String[actual.rows().length];
        for (int i = 0; i < actual.rows().length; i++) {
            Object[] row = actual.rows()[i];
            resultRows[i] = TestingHelpers.printRow(row);
        }
        assertThat(resultRows).containsExactly(rows);
        return this;
    }

    public SQLResponseAssert hasRows(Object[] ... rows) {
        assertThat(List.of(actual.rows())).containsExactly(rows);
        return this;
    }

    public SQLResponseAssert hasRowsInAnyOrder(Object[] ... rows) {
        assertThat(List.of(actual.rows())).containsExactlyInAnyOrder(rows);
        return this;
    }

    public SQLResponseAssert hasColumns(String ... names) {
        assertThat(actual.cols()).containsExactly(names);
        return this;
    }

    public SQLResponseAssert isEmpty() {
        assertThat(actual.rows()).isEmpty();
        return this;
    }
}
