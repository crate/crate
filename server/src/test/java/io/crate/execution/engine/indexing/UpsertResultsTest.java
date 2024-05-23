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

package io.crate.execution.engine.indexing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Map;

import org.junit.Test;

import io.crate.data.Row;
import io.crate.execution.engine.indexing.UpsertResults.Result;
import io.crate.testing.TestingHelpers;

public class UpsertResultsTest {

    @Test
    public void test_error_line_numbers_are_limited_to_50() throws Exception {
        UpsertResults upsertResults = new UpsertResults();
        for (int i = 0; i < 60; i++) {
            upsertResults.addResult("dummyUri", "some failure", i);
        }

        Iterable<Row> rows = upsertResults.rowsIterable();
        assertThat(TestingHelpers.printedTable(rows)).isEqualTo("NULL| dummyUri| 0| 60| {some failure={count=60, line_numbers=[" +
            "0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, " +
            "19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, " +
            "36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49]}}\n");
    }

    @Test
    public void testContainsAnyErrorMethod() {
        UpsertResults upsertResults = new UpsertResults();
        assertThat(upsertResults.containsErrors()).isFalse();
        upsertResults.addResult(1, null);
        assertThat(upsertResults.containsErrors()).isFalse();
        upsertResults.addResult("dummyUri2", "failure test", 1);
        assertThat(upsertResults.containsErrors()).isTrue();
    }

    @Test
    public void testResultToFailureMessageFormat() {
        UpsertResults upsertResults = new UpsertResults();
        assertThat(UpsertResults.resultsToFailure(upsertResults).getMessage()).isEqualTo("Job killed. ");

        upsertResults.addResult("file:///t5.json", "failed to parse", 5);
        upsertResults.addResult("file:///t6.json", "failed to parse", 6);
        assertThat(UpsertResults.resultsToFailure(upsertResults).getMessage()).isEqualTo("""
                                                                                      Job killed.\s
                                                                                      [URI: file:///t6.json, ERRORS: {failed to parse={count=1, line_numbers=[6]}}],
                                                                                      [URI: file:///t5.json, ERRORS: {failed to parse={count=1, line_numbers=[5]}}]""");

        upsertResults = new UpsertResults(Map.of("id", "RMU1uSbNQCijZR6PqtJEKg", "name", "Alplerspitz"));
        assertThat(UpsertResults.resultsToFailure(upsertResults).getMessage()).isEqualTo("Job killed. NODE: Alplerspitz");

        upsertResults.addResult("file:///t.json",
                                "mapping set to strict, dynamic introduction of [b] within [default] is not allowed",
                                2);
        upsertResults.addResult("file:///t.json",
                                "mapping set to strict, dynamic introduction of [b] within [default] is not allowed",
                                3);
        upsertResults.addResult("file:///t2.json", "failed to parse", 4);
        upsertResults.addResult("file:///t2.json", "failed to parse", 5);
        String message = UpsertResults.resultsToFailure(upsertResults).getMessage();
        assertThat(
            message, containsString(
                """
                    Job killed. NODE: Alplerspitz
                    [URI: file:///t.json, ERRORS: {mapping set to strict, dynamic introduction of [b] within [default] is not allowed={count=2, line_numbers=[2, 3]}}],
                    [URI: file:///t2.json, ERRORS: {failed to parse={count=2, line_numbers=[4, 5]}}]"""
            )
        );
    }

    @Test
    public void testResultToFailureSuccessMessages() {
        UpsertResults upsertResults = new UpsertResults();
        upsertResults.addResult("file:///t5.json", null, 5);
        assertThat(UpsertResults.resultsToFailure(upsertResults).getMessage()).isEqualTo("Job killed. \n[URI: file:///t5.json, ERRORS: {}]");
    }

    @Test
    public void test_error_messages_are_limited_to_25() throws Exception {
        UpsertResults upsertResults = new UpsertResults();
        for (int i = 0; i < 30; i++) {
            upsertResults.addResult("dummyUri", "error-" + i, i);
        }
        Result result = upsertResults.getResultSafe("dummyUri");
        assertThat(result.errors).hasSize(25);
    }
}
