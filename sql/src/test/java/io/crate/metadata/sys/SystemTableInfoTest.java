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

package io.crate.metadata.sys;

import io.crate.metadata.Reference;
import io.crate.metadata.table.TableInfo;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.TestingHelpers;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Locale;
import java.util.concurrent.TimeUnit;

public class SystemTableInfoTest extends CrateUnitTest {

    private SysSchemaInfo sysSchemaInfo;
    private TestThreadPool threadPool;

    @Before
    public void prepare() throws Exception {
        threadPool = new TestThreadPool("dummy");
        sysSchemaInfo = new SysSchemaInfo(ClusterServiceUtils.createClusterService(threadPool));
    }

    @After
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }

    @Test
    public void testColumnsInAlphabeticalColumnOrder() throws Exception {
        for (TableInfo tableInfo : sysSchemaInfo) {
            assertSortedColumns(tableInfo);
        }
    }

    private void assertSortedColumns(TableInfo tableInfo) {
        assertThat(String.format(Locale.ENGLISH, "columns from iterator of table %s not in alphabetical order", tableInfo.ident().fqn()),
            tableInfo,
            TestingHelpers.isSortedBy(Reference.TO_COLUMN_IDENT));
        assertThat(String.format(
            Locale.ENGLISH, "columns of table %s not in alphabetical order", tableInfo.ident().fqn()),
            tableInfo.columns(),
            TestingHelpers.isSortedBy(Reference.TO_COLUMN_IDENT));
    }
}
