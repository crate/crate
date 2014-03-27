/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operation.collect.files;

import com.google.common.collect.ImmutableMap;
import io.crate.DataType;
import io.crate.metadata.DynamicFunctionResolver;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.Functions;
import io.crate.operation.projectors.CollectingProjector;
import io.crate.operation.reference.file.FileLineReferenceResolver;
import org.apache.lucene.util.BytesRef;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;

import static io.crate.testing.TestingHelpers.createReference;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;


public class FileReadingCollectorTest {

    private File tmpFile;
    private FileCollectInputSymbolVisitor inputSymbolVisitor;

    @Before
    public void setUp() throws Exception {
        tmpFile = File.createTempFile("fileReadingCollector", ".json");
        try (FileWriter writer = new FileWriter(tmpFile)) {
            writer.write("{\"name\": \"Arthur\", \"id\": 4, \"details\": {\"age\": 38}}\n");
            writer.write("{\"id\": 5, \"name\": \"Trillian\", \"details\": {\"age\": 33}}\n");
        }

        Functions functions = new Functions(
                ImmutableMap.<FunctionIdent, FunctionImplementation>of(),
                ImmutableMap.<String, DynamicFunctionResolver>of()
        );
        inputSymbolVisitor =
                new FileCollectInputSymbolVisitor(functions, FileLineReferenceResolver.INSTANCE);
    }

    @Test
    public void testDoCollectRaw() throws Exception {
        CollectingProjector projector = new CollectingProjector();
        FileCollectInputSymbolVisitor.Context context =
                inputSymbolVisitor.process(createReference("_raw", DataType.STRING));

        FileReadingCollector collector = new FileReadingCollector(
                tmpFile.getAbsolutePath(),
                context.topLevelInputs(),
                context.expressions(),
                projector,
                FileReadingCollector.FileFormat.JSON,
                false
        );

        projector.startProjection();
        collector.doCollect();

        Object[][] rows = projector.getRows();
        assertThat(((BytesRef)rows[0][0]).utf8ToString(), is(
                "{\"name\": \"Arthur\", \"id\": 4, \"details\": {\"age\": 38}}"));
        assertThat(((BytesRef)rows[1][0]).utf8ToString(), is(
                "{\"id\": 5, \"name\": \"Trillian\", \"details\": {\"age\": 33}}"));
    }
}
