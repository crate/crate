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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

public class URLFileInputTest extends ESTestCase {

    @Test
    public void testGetStream() throws IOException {
        Path tempDir = createTempDir();
        File file = new File(tempDir.toFile(), "doesnt_exist");
        URLFileInput input = new URLFileInput(file.toURI());

        String expectedMessage = "doesnt_exist (No such file or directory)";
        if (isRunningOnWindows()) {
            expectedMessage = "doesnt_exist (The system cannot find the file specified)";
        }

        assertThatThrownBy(() -> input.getStream(file.toURI()))
            .isExactlyInstanceOf(FileNotFoundException.class)
            .hasMessageContaining(expectedMessage);
    }
}
