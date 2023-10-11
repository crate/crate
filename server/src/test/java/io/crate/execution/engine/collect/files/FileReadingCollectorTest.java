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

package io.crate.execution.engine.collect.files;


import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import io.crate.execution.engine.collect.files.FileReadingIterator.LineCursor;

public class FileReadingCollectorTest extends ESTestCase {
    private static ThreadPool THREAD_POOL;
    private static File tmpFile;
    private static File tmpFileGz;
    private static File tmpFileEmptyLine;

    private static String line1 = "{\"name\": \"Arthur\", \"id\": 4, \"details\": {\"age\": 38}}";
    private static String line2 = "{\"id\": 5, \"name\": \"Trillian\", \"details\": {\"age\": 33}}";

    private static LineCursor[] expectedResult(File file) throws Exception {
        return new LineCursor[] {
            new LineCursor(fileToURI(file), 1, line1, null),
            new LineCursor(fileToURI(file), 2, line2, null)
        };
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
        Path copy_from = Files.createTempDirectory("copy_from");
        Path copy_from_gz = Files.createTempDirectory("copy_from_gz");
        Path copy_from_empty = Files.createTempDirectory("copy_from_empty");
        tmpFileGz = File.createTempFile("fileReadingCollector", ".json.gz", copy_from_gz.toFile());
        tmpFile = File.createTempFile("fileReadingCollector", ".json", copy_from.toFile());
        tmpFileEmptyLine = File.createTempFile("emptyLine", ".json", copy_from_empty.toFile());
        try (BufferedWriter writer =
                 new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(tmpFileGz)),
                     StandardCharsets.UTF_8))) {
            writer.write(line1);
            writer.write("\n");
            writer.write(line2);
            writer.write("\n");
        }
        try (OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(tmpFile), StandardCharsets.UTF_8)) {
            writer.write(line1);
            writer.write("\n");
            writer.write(line2);
            writer.write("\n");
        }
        try (OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(tmpFileEmptyLine), StandardCharsets.UTF_8)) {
            writer.write(line1);
            writer.write("\n");
            writer.write("\n");
            writer.write(line2);
            writer.write("\n");
        }
        THREAD_POOL = new TestThreadPool(Thread.currentThread().getName());
    }


    @AfterClass
    public static void tearDownClass() throws Exception {
        assertThat(tmpFile.delete()).isTrue();
        assertThat(tmpFileGz.delete()).isTrue();
        assertThat(tmpFileEmptyLine.delete()).isTrue();
        ThreadPool.terminate(THREAD_POOL, 30, TimeUnit.SECONDS);
    }

    @Test
    public void testUmlautsAndWhitespacesWithExplicitURIThrowsAre() throws Throwable {
        assertThatThrownBy(() -> collect("file:///this will fäil.json"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Illegal character in path at index 12: file:///this will fäil.json");
    }

    @Test
    public void testNoErrorIfNoSuchFile() throws Throwable {
        assertThat(collect("file:///some/path/that/shouldnt/exist/foo.json")).satisfiesExactly(
            line1 -> assertThat(line1.failure()).hasMessageContaining("No such file or directory")
        );
        assertThat(collect("file:///some/path/that/shouldnt/exist/*")).isEmpty();
    }

    @Test
    public void testRelativeImport() throws Throwable {
        assertThatThrownBy(() -> collect("xy"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("relative fileURIs are not allowed");
    }

    @Test
    public void testCollectFromUriWithGlob() throws Throwable {
        List<LineCursor> result = collect(fileToURI(tmpFile.getParentFile()) + "file*.json");
        assertThat(result).containsExactly(expectedResult(tmpFile));
    }

    @Test
    public void testCollectFromDirectory() throws Throwable {
        List<LineCursor> result = collect(fileToURI(tmpFile.getParentFile()) + "*");
        assertThat(result).containsExactly(expectedResult(tmpFile));
    }

    @Test
    public void test_collect_exact_uri() throws Throwable {
        List<LineCursor> result = collect(fileToURI(tmpFile).toString());
        assertThat(result).containsExactly(expectedResult(tmpFile));
    }

    @Test
    public void testDoCollectRawFromCompressed() throws Throwable {
        List<LineCursor> result = collect(Collections.singletonList(fileToURI(tmpFileGz).toString()), "gzip");
        assertThat(result).containsExactly(expectedResult(tmpFileGz));
    }

    @Test
    public void testCollectWithEmptyLine() throws Throwable {
        List<LineCursor> result = collect(fileToURI(tmpFileEmptyLine).toString());
        assertThat(result).containsExactly(
            new LineCursor(fileToURI(tmpFileEmptyLine), 1, "{\"name\": \"Arthur\", \"id\": 4, \"details\": {\"age\": 38}}", null),
            new LineCursor(fileToURI(tmpFileEmptyLine), 3, "{\"id\": 5, \"name\": \"Trillian\", \"details\": {\"age\": 33}}", null)
        );
    }

    @Test
    public void unsupportedURITest() throws Throwable {
        FileReadingIterator it = it("invalid://crate.io/docs/en/latest/sql/reference/copy_from.html");
        LineCursor currentElement = it.currentElement();
        assertThat(it.moveNext()).isTrue();
        assertThat(currentElement.lineNumber()).isEqualTo(0);
        assertThat(currentElement.line()).isNull();
        assertThat(currentElement.failure()).hasMessage("unknown protocol: invalid");
    }

    @Test
    public void testMultipleUriSupport() throws Throwable {
        List<String> fileUris = new ArrayList<>();
        fileUris.add(Paths.get(tmpFile.toURI()).toUri().toString());
        fileUris.add(Paths.get(tmpFileEmptyLine.toURI()).toUri().toString());
        List<LineCursor> results = collect(fileUris, null);
        assertThat(results).containsExactly(
            new LineCursor(tmpFile.toURI(), 1, "{\"name\": \"Arthur\", \"id\": 4, \"details\": {\"age\": 38}}", null),
            new LineCursor(tmpFile.toURI(), 2, "{\"id\": 5, \"name\": \"Trillian\", \"details\": {\"age\": 33}}", null),
            new LineCursor(tmpFileEmptyLine.toURI(), 1, "{\"name\": \"Arthur\", \"id\": 4, \"details\": {\"age\": 38}}", null),
            new LineCursor(tmpFileEmptyLine.toURI(), 3, "{\"id\": 5, \"name\": \"Trillian\", \"details\": {\"age\": 33}}", null)
        );
    }

    private static List<LineCursor> collect(String ... fileUris) throws Exception {
        return collect(Arrays.asList(fileUris), null);
    }

    private static FileReadingIterator it(String ... fileUris) {
        return it(Arrays.asList(fileUris), null);
    }

    private static FileReadingIterator it(Collection<String> fileUris, String compression) {
        return new FileReadingIterator(
            fileUris,
            compression,
            Map.of(LocalFsFileInputFactory.NAME, new LocalFsFileInputFactory()),
            false,
            1,
            0,
            Settings.EMPTY,
            THREAD_POOL.scheduler());
    }

    private static List<LineCursor> collect(Collection<String> fileUris, String compression) throws Exception {
        return it(fileUris, compression)
            .map(LineCursor::copy)
            .toList()
            .get(5, TimeUnit.SECONDS);
    }

    private static URI fileToURI(File file) throws IOException {
        return file.toPath().toRealPath().toUri();
    }
}
