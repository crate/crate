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

package io.crate.integrationtests;

import io.crate.testing.TestingHelpers;
import io.crate.testing.UseJdbc;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
@UseJdbc
public class UnionIntegrationTest extends SQLTransportIntegrationTest {

    @Test
    public void testUnionAll2Tables() throws Exception {
        createColorsAndSizes();
        execute("select * from colors " +
                "union all " +
                "select * from sizes");
        assertThat(Arrays.asList(response.rows()), containsInAnyOrder(new Object[]{"red"},
                                                                      new Object[]{"blue"},
                                                                      new Object[]{"green"},
                                                                      new Object[]{"small"},
                                                                      new Object[]{"large"}));
    }

    @Test
    public void testUnionAll3Tables() throws Exception {
        createColorsAndSizes();
        execute("select * from sizes " +
                "union all " +
                "select * from colors " +
                "union all " +
                "select * from sizes");
        assertThat(Arrays.asList(response.rows()), containsInAnyOrder(new Object[]{"small"},
                                                                      new Object[]{"large"},
                                                                      new Object[]{"red"},
                                                                      new Object[]{"blue"},
                                                                      new Object[]{"green"},
                                                                      new Object[]{"small"},
                                                                      new Object[]{"large"}));
    }

    @Test
    public void testUnionAllWithOrderBy() throws Exception {
        createColorsAndSizes();
        execute("select * from colors " +
                "union all " +
                "select * from sizes " +
                "order by 1");
        assertThat(TestingHelpers.printedTable(response.rows()), is("blue\n" +
                                                                    "green\n" +
                                                                    "large\n" +
                                                                    "red\n" +
                                                                    "small\n"));
    }

    private void createColorsAndSizes() {
        execute("create table colors (name string) ");
        execute("create table sizes (name string) ");
        ensureYellow();

        execute("insert into colors (name) values (?)", new Object[][]{
            new Object[]{"red"},
            new Object[]{"blue"},
            new Object[]{"green"}
        });
        execute("insert into sizes (name) values (?)", new Object[][]{
            new Object[]{"small"},
            new Object[]{"large"},
        });
        execute("refresh table colors, sizes");
    }
}
