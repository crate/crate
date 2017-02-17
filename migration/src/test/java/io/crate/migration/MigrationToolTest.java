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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

public class MigrationToolTest extends MigrationTestCase {

    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none();

    @Test
    public void testWrongArgs() throws IOException {
        ByteArrayOutputStream outByteArrayOutputStream = new ByteArrayOutputStream();
        ByteArrayOutputStream errByteArrayOutputStream = new ByteArrayOutputStream();
        PrintStream outStream = new PrintStream(outByteArrayOutputStream);
        PrintStream errStream = new PrintStream(errByteArrayOutputStream);
        System.setOut(outStream);
        System.setErr(errStream);
        exit.checkAssertionAfterwards(() -> assertThat(outByteArrayOutputStream.toString(), is(MigrationTool.USAGE)));
        exit.checkAssertionAfterwards(() -> assertThat(errByteArrayOutputStream.toString(),
            is("ERROR: Wrong argument provided: foo" + System.lineSeparator())));
        exit.expectSystemExitWithStatus(1);
        MigrationTool.main(new String[]{"foo"});
    }

    @Test
    public void testHelpArgs() throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(byteArrayOutputStream);
        System.setOut(printStream);
        exit.checkAssertionAfterwards(() -> assertThat(byteArrayOutputStream.toString(), is(MigrationTool.USAGE)));
        exit.expectSystemExitWithStatus(0);
        MigrationTool.main(new String[]{"-h"});
    }

    @Test
    public void testRetrieveIndexDirs() throws IOException {
        String[] tableNames = new String[]{"table1", "table2", "table3", "table4", "table5"};

        File[] dirs = new File[7];
        Path indexDir = Files.createTempDirectory("");
        dirs[0] = new File(Files.createDirectory(indexDir.resolve("table1")).toUri());
        dirs[1] = new File(Files.createDirectory(indexDir.resolve("customSchema.table2")).toUri());
        dirs[2] = new File(Files.createDirectory(indexDir.resolve(".partitioned.table3.asdf1")).toUri());
        dirs[3] = new File(Files.createDirectory(indexDir.resolve(".partitioned.table3.fsda2")).toUri());
        dirs[4] = new File(Files.createDirectory(indexDir.resolve("foo..partitioned.table4.vcxz2")).toUri());
        dirs[5] = new File(Files.createDirectory(indexDir.resolve("foo..partitioned.table4.zxcv1")).toUri());
        dirs[6] = new File(Files.createDirectory(indexDir.resolve("foo..partitioned.table5.zxcv1")).toUri());

        Map<Table, List<File>> tableDirs = new HashMap<>();
        MigrationTool.retrieveIndexDirs(tableDirs, indexDir.toString(), ImmutableSet.of());
        assertThat(tableDirs.size(), is(tableNames.length));

        for (Map.Entry<Table, List<File>> entry : tableDirs.entrySet()) {
            Table table = entry.getKey();
            if (table.name().equals("doc." + tableNames[0])) {
                assertThat(table.isPartitioned(), is(false));
                assertThat(entry.getValue(), containsInAnyOrder(dirs[0]));
            } else if (table.name().equals("customSchema." + tableNames[1])) {
                assertThat(table.isPartitioned(), is(false));
                assertThat(entry.getValue(), containsInAnyOrder(dirs[1]));
            } else if (table.name().equals("doc." + tableNames[2])) {
                assertThat(table.isPartitioned(), is(true));
                assertThat(entry.getValue(), containsInAnyOrder(dirs[2], dirs[3]));
            } else if (table.name().equals("foo." + tableNames[3])) {
                assertThat(table.isPartitioned(), is(true));
                assertThat(entry.getValue(), containsInAnyOrder(dirs[4], dirs[5]));
            } else if (table.name().equals("foo." + tableNames[4])) {
                assertThat(table.isPartitioned(), is(true));
                assertThat(entry.getValue(), containsInAnyOrder(dirs[6]));
            }
        }
    }

    @Test
    public void testRetrieveIndexDirsFiltered() throws IOException {
        String[] tableNames = new String[]{"table1", "table2", "table3", "table4"};

        File[] dirs = new File[9];
        Path indexDir = Files.createTempDirectory("");
        dirs[0] = new File(Files.createDirectory(indexDir.resolve("table1")).toUri());
        dirs[1] = new File(Files.createDirectory(indexDir.resolve("customSchema.table2")).toUri());
        dirs[2] = new File(Files.createDirectory(indexDir.resolve(".partitioned.table3.asdf1")).toUri());
        dirs[3] = new File(Files.createDirectory(indexDir.resolve(".partitioned.table3.fsda2")).toUri());
        dirs[4] = new File(Files.createDirectory(indexDir.resolve("foo..partitioned.table4.vcxz2")).toUri());
        dirs[5] = new File(Files.createDirectory(indexDir.resolve("foo..partitioned.table4.zxcv1")).toUri());
        dirs[6] = new File(Files.createDirectory(indexDir.resolve("foo..partitioned.table5.zxcv1")).toUri());
        dirs[7] = new File(Files.createDirectory(indexDir.resolve("table6")).toUri());
        dirs[8] = new File(Files.createDirectory(indexDir.resolve("custom.table7")).toUri());

        Map<Table, List<File>> tableDirs = new HashMap<>();
        MigrationTool.retrieveIndexDirs(tableDirs, indexDir.toString(), Sets.newHashSet(tableNames));
        assertThat(tableDirs.size(), is(tableNames.length));

        for (Map.Entry<Table, List<File>> entry : tableDirs.entrySet()) {
            Table table = entry.getKey();
            if (table.name().equals("doc." + tableNames[0])) {
                assertThat(table.isPartitioned(), is(false));
                assertThat(entry.getValue(), containsInAnyOrder(dirs[0]));
            } else if (table.name().equals("customSchema." + tableNames[1])) {
                assertThat(table.isPartitioned(), is(false));
                assertThat(entry.getValue(), containsInAnyOrder(dirs[1]));
            } else if (table.name().equals("doc." + tableNames[2])) {
                assertThat(table.isPartitioned(), is(true));
                assertThat(entry.getValue(), containsInAnyOrder(dirs[2], dirs[3]));
            } else if (table.name().equals("foo." + tableNames[3])) {
                assertThat(table.isPartitioned(), is(true));
                assertThat(entry.getValue(), containsInAnyOrder(dirs[4], dirs[5]));
            }
        }
    }

    @Test
    public void testIndexLocked() throws IOException {
        Path zippedIndexDir = getDataPath("/indices/cratehome_index_locked.zip");
        Path dataDir = prepareIndexDir(zippedIndexDir);
        Path lockFilePath = dataDir.resolve("data").resolve("crate").resolve("nodes").resolve("0").resolve("node.lock");
        FileChannel fileChannel = FileChannel.open(lockFilePath, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        FileLock fileLock = fileChannel.lock();
        System.setProperty("es.path.home", dataDir.toString());
        System.setProperty("es.node.max_local_storage_nodes", "0");

        ByteArrayOutputStream outByteArrayOutputStream = new ByteArrayOutputStream();
        PrintStream outStream = new PrintStream(outByteArrayOutputStream);
        System.setOut(outStream);
        exit.expectSystemExitWithStatus(1);
        exit.checkAssertionAfterwards(() -> {
            fileLock.release();
            fileLock.close();
            fileChannel.close();
        });
        MigrationTool.main(new String[]{});
    }

    @Test
    public void testReindexRequired() throws IOException {
        Path zippedIndexDir = getDataPath("/indices/cratehome_reindex_required.zip");
        Path dataDir = prepareIndexDir(zippedIndexDir);
        System.setProperty("es.path.home", dataDir.toString());
        System.setProperty("es.node.max_local_storage_nodes", "3");

        ByteArrayOutputStream outByteArrayOutputStream = new ByteArrayOutputStream();
        PrintStream outStream = new PrintStream(outByteArrayOutputStream);
        System.setOut(outStream);
        exit.checkAssertionAfterwards(() -> assertThat(outByteArrayOutputStream.toString().contains(
            ("\n" +
             "-------------\n" +
             "-- SUMMARY --\n" +
             "-------------\n" +
             "\n" +
             "Tables that require reindexing: doc.testneedsreindex[node0, node1, node2]," +
             " doc.testneedsreindex_parted[node0, node1, node2]\n\n")), is(true)));
        exit.expectSystemExitWithStatus(0);
        MigrationTool.main(new String[]{});
    }
}
