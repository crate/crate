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

package io.crate.execution.engine.aggregation.impl;

import static io.crate.testing.Asserts.assertThat;

import org.elasticsearch.test.IntegTestCase;

import io.crate.testing.UseJdbc;

public class TopKIntegrationTest extends IntegTestCase {

    @UseJdbc(1)
    public void test_top_k_agg_with_default_limit() {
        execute("select top_k(country) from sys.summits;");
        assertThat(response)
            .hasRows("[" +
                "{item=IT, frequency=436}, " +
                "{item=AT, frequency=401}, " +
                "{item=CH, frequency=320}, " +
                "{item=FR, frequency=240}, " +
                "{item=CH/IT, frequency=60}, " +
                "{item=FR/IT, frequency=43}, " +
                "{item=AT/IT, frequency=30}, " +
                "{item=SI, frequency=22}" +
                "]"
            );
    }

    @UseJdbc(1)
    public void test_top_k_agg_with_custom_limit() {
        execute("select top_k(country, 4) from sys.summits;");
        assertThat(response)
            .hasRows("[" +
                "{item=IT, frequency=436}, " +
                "{item=AT, frequency=401}, " +
                "{item=CH, frequency=320}, " +
                "{item=FR, frequency=240}" +
                "]"
            );
    }
}
