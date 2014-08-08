/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.analyze;

import com.google.common.base.Joiner;
import io.crate.sql.tree.*;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class MatchOptionsAnalysisTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testMatchOptions() throws Exception {
        GenericProperties props = new GenericProperties();
        props.add(new GenericProperty("analyzer", new StringLiteral("english")));
        props.add(new GenericProperty("operator", new StringLiteral("and")));
        props.add(new GenericProperty("fuzziness", new ParameterExpression(1)));
        Map<String, Object> processed = MatchOptionsAnalysis.process(props, new Object[]{12});
        assertThat(
                Joiner.on(", ").withKeyValueSeparator(":").join(processed),
                is("fuzziness:12, operator:and, analyzer:english"));
    }

    @Test
    public void testUnknownMatchOptions() throws Exception {

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("unknown match option 'analyzer_wrong'");

        GenericProperties props = new GenericProperties();
        props.add(new GenericProperty("prefix_length", new LongLiteral("4")));
        props.add(new GenericProperty("fuzziness", new ParameterExpression(1)));
        props.add(new GenericProperty("analyzer_wrong", new StringLiteral("english")));

        MatchOptionsAnalysis.process(props, new Object[]{12});
    }

    @Test
    public void testInvalidMatchValue() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("invalid value for option 'max_expansions': abc");

        GenericProperties props = new GenericProperties();
        props.add(new GenericProperty("fuzziness", new ParameterExpression(1)));
        props.add(new GenericProperty("max_expansions", new StringLiteral("abc")));
        props.add(new GenericProperty("analyzer", new StringLiteral("english")));

        MatchOptionsAnalysis.process(props, new Object[]{""});
    }

}
