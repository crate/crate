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

package io.crate.window;

import io.crate.integrationtests.SQLIntegrationTestCase;
import org.elasticsearch.plugins.Plugin;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

import static io.crate.testing.TestingHelpers.printedTable;
import static org.hamcrest.Matchers.is;

public class NthValueFunctionIntegrationTest extends SQLIntegrationTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        ArrayList<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(EnterpriseFunctionsProxyTestPlugin.class);
        return plugins;
    }

    @Test
    public void testGeneralPurposeWindowFunctionsWithStandaloneValues() {
        execute("select col1, col2, " +
                "first_value(col1) OVER(order by col2, col1), " +
                "last_value(col1) OVER(order by col2, col1), " +
                "nth_value(col1, 3) OVER(order by col2, col1) " +
                "from unnest([1, 1, 3, 2], [1, 2, 2, 3]) " +
                "order by col1, col2");
        assertThat(printedTable(response.rows()), is("1| 1| 1| 1| NULL\n" +
                                                     "1| 2| 1| 1| NULL\n" +
                                                     "2| 3| 1| 2| 3\n" +
                                                     "3| 2| 1| 3| 3\n"));
    }

    @Test
    public void testLastValueWithScalar() {
        execute(
            "select col1, last_value(col1) OVER(order by col2, col1), last_value(char_length(col1)) OVER(order by col2, col1) " +
            "from unnest(['a', 'cc', 'd', 'cc', 'b'], [1, 2, 2, 3, 1]) order by col1, col2");
        assertThat(printedTable(response.rows()), is("a| a| 1\n" +
                                                     "b| b| 1\n" +
                                                     "cc| cc| 2\n" +
                                                     "cc| cc| 2\n" +
                                                     "d| d| 1\n"));
    }
}
