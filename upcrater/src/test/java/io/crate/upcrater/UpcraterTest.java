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

package io.crate.upcrater;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.lucene.util.TestUtil;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

public class UpcraterTest {

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
        exit.checkAssertionAfterwards(() -> assertThat(outByteArrayOutputStream.toString(), is(Upcrater.USAGE)));
        exit.checkAssertionAfterwards(() -> assertThat(errByteArrayOutputStream.toString(),
            is("ERROR: Wrong argument provided: foo" + System.lineSeparator())));
        exit.expectSystemExitWithStatus(1);
        Upcrater.main(new String[]{"foo"});
    }

    @Test
    public void testHelpArgs() throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(byteArrayOutputStream);
        System.setOut(printStream);
        exit.checkAssertionAfterwards(() -> assertThat(byteArrayOutputStream.toString(), is(Upcrater.USAGE)));
        exit.expectSystemExitWithStatus(0);
        Upcrater.main(new String[]{"-h"});
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
        Upcrater.retrieveIndexDirs(tableDirs, indexDir.toString(), ImmutableSet.of());
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
        Upcrater.retrieveIndexDirs(tableDirs, indexDir.toString(), Sets.newHashSet(tableNames));
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
        Path zippedIndexDir = getDataPath("/data_dirs/cratehome_index_locked.zip");
        Path dataDir = prepareDataDir(zippedIndexDir);
        Path lockFilePath = dataDir.resolve("data").resolve("crate").resolve("nodes").resolve("0").resolve("node.lock");
        FileChannel fileChannel = FileChannel.open(lockFilePath, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        FileLock fileLock = fileChannel.lock();
        System.setProperty("es.path.home", dataDir.toString());
        System.setProperty("es.node.max_local_storage_nodes", "5");

        ByteArrayOutputStream outByteArrayOutputStream = new ByteArrayOutputStream();
        PrintStream outStream = new PrintStream(outByteArrayOutputStream);
        System.setOut(outStream);
        exit.expectSystemExitWithStatus(1);
        exit.checkAssertionAfterwards(() -> {
            fileLock.release();
            fileLock.close();
            fileChannel.close();
        });
        Upcrater.main(new String[]{});
    }

    @Test
    public void testReindexRequired() throws IOException {
        Path zippedIndexDir = getDataPath("/data_dirs/cratehome_reindex_required.zip");
        Path dataDir = prepareDataDir(zippedIndexDir);
        System.setProperty("es.path.home", dataDir.toString());
        System.setProperty("es.node.max_local_storage_nodes", "5");

        ByteArrayOutputStream outByteArrayOutputStream = new ByteArrayOutputStream();
        PrintStream outStream = new PrintStream(outByteArrayOutputStream);
        System.setOut(outStream);
        exit.checkAssertionAfterwards(() -> assertThat(outByteArrayOutputStream.toString().contains(
            ("-------------\n" +
             "-- SUMMARY --\n" +
             "-------------\n" +
             "\n" +
             "\uD83D\uDC18 Tables that require re-indexing:   doc.testneedsreindex[node0, node1, node2], doc.testneedsreindex_parted[node0, node1, node2]" +
             "\n")), is(true)));
        exit.expectSystemExitWithStatus(0);
        Upcrater.main(new String[]{});
    }

    @Test
    public void testAlreadyUpgraded() throws IOException {
        Path zippedIndexDir = getDataPath("/data_dirs/cratehome_already_upgraded.zip");
        Path dataDir = prepareDataDir(zippedIndexDir);
        System.setProperty("es.path.home", dataDir.toString());
        System.setProperty("es.node.max_local_storage_nodes", "5");

        ByteArrayOutputStream outByteArrayOutputStream = new ByteArrayOutputStream();
        PrintStream outStream = new PrintStream(outByteArrayOutputStream);
        System.setOut(outStream);
        exit.checkAssertionAfterwards(() -> assertThat(outByteArrayOutputStream.toString().contains(
            ("-------------\n" +
             "-- SUMMARY --\n" +
             "-------------\n" +
             "\n" +
             "\uD83D\uDC18 Tables already upgraded:             doc.testalreadyupgraded[node0, node1, node2], doc.testalreadyupgraded_parted[node0, node1, node2]\n" +
             "\n" +
             "\uD83D\uDC18  All is up-to-date. Happy data crunching!  \uD83D\uDC18\n")), is(true)));
        exit.expectSystemExitWithStatus(0);
        Upcrater.main(new String[]{});
    }

    @Test
    public void testUpgradeFailed() throws IOException {
        Path zippedIndexDir = getDataPath("/data_dirs/cratehome_failed.zip");
        Path dataDir = prepareDataDir(zippedIndexDir);
        System.setProperty("es.path.home", dataDir.toString());
        System.setProperty("es.node.max_local_storage_nodes", "5");

        ByteArrayOutputStream outByteArrayOutputStream = new ByteArrayOutputStream();
        PrintStream outStream = new PrintStream(outByteArrayOutputStream);
        System.setOut(outStream);
        exit.checkAssertionAfterwards(() -> assertThat(outByteArrayOutputStream.toString().contains(
            ("-------------\n" +
             "-- SUMMARY --\n" +
             "-------------\n" +
             "\n" +
             "\uD83D\uDC18 Tables errored while upgrading (pls check error logs for details):     doc.testfailed[node0, node2], doc.testfailed_parted[node0, node1, node2]\n" +
             "\n")), is(true)));
        exit.expectSystemExitWithStatus(0);
        Upcrater.main(new String[]{});
    }

    @Test
    public void testUpgradeRequired() throws IOException {
        Path zippedIndexDir = getDataPath("/data_dirs/cratehome_upgrade_required.zip");
        Path dataDir = prepareDataDir(zippedIndexDir);
        System.setProperty("es.path.home", dataDir.toString());
        System.setProperty("es.node.max_local_storage_nodes", "5");

        ByteArrayOutputStream outByteArrayOutputStream = new ByteArrayOutputStream();
        PrintStream outStream = new PrintStream(outByteArrayOutputStream);
        System.setOut(outStream);
        exit.checkAssertionAfterwards(() -> assertThat(outByteArrayOutputStream.toString().contains(
            ("-------------\n" +
             "-- SUMMARY --\n" +
             "-------------\n" +
             "\n" +
             "\uD83D\uDC18 Tables successfully upgraded:      doc.testneedsupgrade[node0, node1, node2], doc.testneedsupgrade_parted[node0, node1, node2]\n" +
             "\n" +
             "\uD83D\uDC18  All is up-to-date. Happy data crunching!  \uD83D\uDC18\n")), is(true)));
        exit.expectSystemExitWithStatus(0);
        Upcrater.main(new String[]{});
    }

    @Test
    public void testUpgradeRequiredDryRun() throws IOException {
        Path zippedIndexDir = getDataPath("/data_dirs/cratehome_upgrade_required.zip");
        Path dataDir = prepareDataDir(zippedIndexDir);
        System.setProperty("es.path.home", dataDir.toString());
        System.setProperty("es.node.max_local_storage_nodes", "5");

        ByteArrayOutputStream outByteArrayOutputStream = new ByteArrayOutputStream();
        PrintStream outStream = new PrintStream(outByteArrayOutputStream);
        System.setOut(outStream);
        exit.checkAssertionAfterwards(() -> assertThat(outByteArrayOutputStream.toString().contains(
            ("-------------\n" +
             "-- SUMMARY --\n" +
             "-------------\n" +
             "\n" +
             "\uD83D\uDC18 Tables to be upgraded:             doc.testneedsupgrade[node0, node1, node2], doc.testneedsupgrade_parted[node0, node1, node2]\n" +
             "\n" +
             "\uD83D\uDC18  All is up-to-date. Happy data crunching!  \uD83D\uDC18\n")), is(true)));
        exit.expectSystemExitWithStatus(0);
        Upcrater.main(new String[]{"--dry-run"});
    }

    private Path prepareDataDir(Path backwardsIndex) throws IOException {
        Path indexDir = Files.createTempDirectory("");
        Path dataDir = indexDir.resolve("test");
        try (InputStream stream = Files.newInputStream(backwardsIndex)) {
            TestUtil.unzip(stream, dataDir);
        }
        assertThat(Files.exists(dataDir), is(true));
        return dataDir;
    }

    private Path getDataPath(String name) throws IOException {
        try {
            return Paths.get(this.getClass().getResource(name).toURI());
        } catch (Exception e) {
            throw new IOException("Cannot find resource: " + name);
        }
    }
}
