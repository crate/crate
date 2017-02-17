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

package io.crate.operation.collect.files;

import com.google.common.collect.ImmutableMap;
import io.crate.data.BatchIterator;
import io.crate.metadata.*;
import io.crate.operation.Input;
import io.crate.operation.InputFactory;
import io.crate.operation.reference.file.FileLineReferenceResolver;
import io.crate.testing.BatchIteratorTester;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static io.crate.testing.TestingHelpers.createReference;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class FileReadingIteratorTest {

    private static File tmpFile;
    private InputFactory inputFactory;

    @BeforeClass
    public static void setUpClass() throws Exception {
        Path copy_from = Files.createTempDirectory("copy_from");
        tmpFile = File.createTempFile("fileReadingCollector", ".json", copy_from.toFile());
        try (OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(tmpFile), StandardCharsets.UTF_8)) {
            writer.write("{\"name\": \"Arthur\", \"id\": 4, \"details\": {\"age\": 38}}\n");
            writer.write("{\"id\": 5, \"name\": \"Trillian\", \"details\": {\"age\": 33}}\n");
        }
    }

    @Before
    public void prepare() throws Exception {
        Functions functions = new Functions(
            ImmutableMap.<FunctionIdent, FunctionImplementation>of(),
            ImmutableMap.<String, FunctionResolver>of()
        );
        inputFactory = new InputFactory(functions);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        assertThat(tmpFile.delete(), is(true));
    }

    @Test
    public void testIteratorContract() throws Exception {
        String fileUri = Paths.get(tmpFile.getParentFile().toURI()).toUri().toString() + "file*.json";
        Supplier<BatchIterator> batchIteratorSupplier = () -> createBatchIterator(
            Collections.singletonList(fileUri), null
        );

        byte[] firstLine = "{\"name\": \"Arthur\", \"id\": 4, \"details\": {\"age\": 38}}".getBytes(StandardCharsets.UTF_8);
        byte[] secondLine = "{\"id\": 5, \"name\": \"Trillian\", \"details\": {\"age\": 33}}".getBytes(StandardCharsets.UTF_8);

        BatchIteratorTester tester = new BatchIteratorTester(
            batchIteratorSupplier,
            Arrays.asList(new Object[]{new BytesRef(firstLine)},
                new Object[]{new BytesRef(secondLine)})
        );
        tester.run();
    }

    private BatchIterator createBatchIterator(Collection<String> fileUris, String compression) {
        Reference raw = createReference("_raw", DataTypes.STRING);
        InputFactory.Context<LineCollectorExpression<?>> ctx =
            inputFactory.ctxForRefs(FileLineReferenceResolver::getImplementation);
        List<Input<?>> inputs = Collections.singletonList(ctx.add(raw));
        return FileReadingIterator.newInstance(
            fileUris,
            inputs,
            ctx.expressions(),
            compression,
            ImmutableMap.of(
                LocalFsFileInputFactory.NAME, new LocalFsFileInputFactory()),
            false,
            1,
            0
        );
    }
}
