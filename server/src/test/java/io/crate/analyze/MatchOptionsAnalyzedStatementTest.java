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

package io.crate.analyze;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

public class MatchOptionsAnalyzedStatementTest extends ESTestCase {

    @Test
    public void testValidOptions() throws Exception {
        Map<String, Object> options = new HashMap<>();
        options.put("analyzer", "english");
        options.put("operator", "and");
        options.put("fuzziness", 12);
        // no exception raised
        MatchOptionsAnalysis.validate(options);
    }

    @Test
    public void testUnknownMatchOptions() throws Exception {
        Map<String, Object> options = new HashMap<>();
        options.put("analyzer_wrong", "english");
        assertThatThrownBy(() -> MatchOptionsAnalysis.validate(options))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("unknown match option 'analyzer_wrong'");
    }

    @Test
    public void testInvalidMatchValue() throws Exception {
        Map<String, Object> options = new HashMap<>();
        options.put("max_expansions", "abc");
        assertThatThrownBy(() -> MatchOptionsAnalysis.validate(options))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("invalid value for option 'max_expansions': abc");
    }

    @Test
    public void testZeroTermsQueryMustBeAString() throws Exception {
        assertThatThrownBy(() -> MatchOptionsAnalysis.validate(Map.of("zero_terms_query", 12.6)))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("invalid value for option 'zero_terms_query': 12.6");
    }

    @Test
    public void testUnknownOption() throws Exception {
        assertThatThrownBy(() -> MatchOptionsAnalysis.validate(Map.of("oh", 1)))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("unknown match option 'oh'");
    }
}
