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

package io.crate.migration;

import io.crate.test.integration.CrateUnitTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

public class MigrationToolTest extends CrateUnitTest {

    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none();

    @Test
    public void testWrongArgs() throws IOException {
        exit.expectSystemExitWithStatus(1);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(byteArrayOutputStream);
        System.setOut(printStream);
        exit.checkAssertionAfterwards(() -> assertThat(byteArrayOutputStream.toString(), is(MigrationTool.USAGE)));
        MigrationTool.main(new String[] {});
    }

    @Test
    public void testRetrieveIndexDirs() throws IOException {
        String[] tableNames = new String[] {"table1", "table2", "table3"};

        File[] dirs = new File[7];
        Path indexDir = createTempDir();
        dirs[0] = new File(Files.createDirectory(indexDir.resolve("table1")).toUri());
        dirs[1] = new File(Files.createDirectory(indexDir.resolve(".partitioned.table2.asdf1")).toUri());
        dirs[2] = new File(Files.createDirectory(indexDir.resolve(".partitioned.table2.fsda2")).toUri());
        dirs[4] = new File(Files.createDirectory(indexDir.resolve("foo..partitioned.table3.vcxz2")).toUri());
        dirs[3] = new File(Files.createDirectory(indexDir.resolve("foo..partitioned.table3.zxcv1")).toUri());
        dirs[5] = new File(Files.createDirectory(indexDir.resolve("foo..partitioned.table4.vcxz2")).toUri());
        dirs[6] = new File(Files.createDirectory(indexDir.resolve("table5")).toUri());

        Map<Table, List<File>> tableDirs = new HashMap<>();
        MigrationTool.retrieveIndexDirs(tableDirs, indexDir.toString(), Arrays.asList(tableNames));
        assertThat(tableDirs.size(), is(tableNames.length));

        for (Map.Entry<Table, List<File>> entry : tableDirs.entrySet()) {
            Table table = entry.getKey();
            if (table.name().equals("doc." + tableNames[0])) {
                assertThat(table.isPartitioned(), is(false));
                assertThat(entry.getValue(), containsInAnyOrder(dirs[0]));
            } else if (table.name().equals("doc." + tableNames[1])) {
                assertThat(table.isPartitioned(), is(true));
                assertThat(entry.getValue(), containsInAnyOrder(dirs[1], dirs[2]));
            } else if (table.name().equals("foo." + tableNames[2])) {
                assertThat(table.isPartitioned(), is(true));
                assertThat(entry.getValue(), containsInAnyOrder(dirs[3], dirs[4]));
            }
        }
    }
}
