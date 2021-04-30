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

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

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
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("unknown match option 'analyzer_wrong'");

        Map<String, Object> options = new HashMap<>();
        options.put("analyzer_wrong", "english");
        MatchOptionsAnalysis.validate(options);
    }

    @Test
    public void testInvalidMatchValue() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("invalid value for option 'max_expansions': abc");

        Map<String, Object> options = new HashMap<>();
        options.put("max_expansions", "abc");
        MatchOptionsAnalysis.validate(options);
    }

    @Test
    public void testZeroTermsQueryMustBeAString() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("invalid value for option 'zero_terms_query': 12.6");
        MatchOptionsAnalysis.validate(Map.of("zero_terms_query", 12.6));
    }

    @Test
    public void testUnknownOption() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("unknown match option 'oh'");
        MatchOptionsAnalysis.validate(Map.of("oh", 1));
    }
}
