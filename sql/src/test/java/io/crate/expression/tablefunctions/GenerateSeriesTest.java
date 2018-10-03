/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.expression.tablefunctions;

import org.junit.Test;

public class GenerateSeriesTest extends AbstractTableFunctionsTest {

    @Test
    public void testFrom3To4WithDefaultStep() {
        assertExecute(
            "generate_series(3, 4)",
            "3\n" +
            "4\n"
        );
    }

    @Test
    public void from2To8With2AsStep() {
        assertExecute(
            "generate_series(2, 8, 2)",
            "2\n" +
            "4\n" +
            "6\n" +
            "8\n"
        );
    }

    @Test
    public void testGenerateSeriesWithIntegerArguments() {
        assertExecute("generate_series(1::int, 3::int)", "1\n2\n3\n");
    }

    @Test
    public void testResultIsEmptyIfStartIsNull() {
        assertExecute("generate_series(null, 10)", "");
    }

    @Test
    public void testResultIsEmptyIfStopIsNull() {
        assertExecute("generate_series(1, null)", "");
    }

    @Test
    public void testResultIsEmptyIfStepIsNull() {
        assertExecute("generate_series(1, 2, null)", "");
    }

    @Test
    public void testStartIsReturnedIfStepIsBiggerThanStop() {
        assertExecute(
            "generate_series(1, 4, 8)",
            "1\n"
        );
    }

    @Test
    public void testEmptyOutputIfStartIsHigherThanStopWithDefaultStep() {
        assertExecute("generate_series(10, 1)", "");
    }

    @Test
    public void testNegativeStepsWorkIfStartIsHigherThanStop() {
        assertExecute(
            "generate_series(4, 1, -1)",
            "4\n" +
            "3\n" +
            "2\n" +
            "1\n");
    }

    @Test
    public void testNegativeStartToPositiveEnd() {
        assertExecute(
            "generate_series(-2, 2, 1)",
            "-2\n" +
            "-1\n" +
            "0\n" +
            "1\n" +
            "2\n"
        );
    }
}
