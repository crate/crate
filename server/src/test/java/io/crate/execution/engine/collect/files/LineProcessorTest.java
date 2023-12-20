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

import static io.crate.role.RolesDefinitions.DEFAULT_USERS;
import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import io.crate.analyze.CopyFromParserProperties;
import io.crate.data.BatchIterator;
import io.crate.data.InMemoryBatchIterator;
import io.crate.execution.dsl.phases.FileUriCollectPhase.InputFormat;
import io.crate.execution.engine.collect.files.FileReadingIterator.LineCursor;
import io.crate.expression.InputFactory;
import io.crate.expression.InputFactory.Context;
import io.crate.expression.reference.file.FileLineReferenceResolver;
import io.crate.expression.reference.file.SourceParsingFailureExpression;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.role.Roles;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataTypes;

public class LineProcessorTest {

    Roles roles = () -> DEFAULT_USERS;
    NodeContext nodeCtx = new NodeContext(new Functions(Map.of()), roles);
    InputFactory inputFactory = new InputFactory(nodeCtx);

    @Test
    public void test_line_processor_parses_json_input() throws Exception {
        URI uri = new URI("file:///dummy.txt");
        BatchIterator<LineCursor> source = InMemoryBatchIterator.of(
            List.of(
                new LineCursor(uri, 1, "{\"x\": 10}", null),
                new LineCursor(uri, 2, "{\"x\": 20}", null)
            ),
            null,
            false
        );
        Context<LineCollectorExpression<?>> ctxForRefs = inputFactory.ctxForRefs(
            CoordinatorTxnCtx.systemTransactionContext(),
            FileLineReferenceResolver::getImplementation
        );
        ctxForRefs.add(List.of(
            TestingHelpers.createReference(DocSysColumns.RAW, DataTypes.STRING),
            TestingHelpers.createReference("x", DataTypes.INTEGER)
        ));

        LineProcessor lineProcessor = new LineProcessor(
            source,
            ctxForRefs.topLevelInputs(),
            ctxForRefs.expressions(),
            InputFormat.JSON,
            new CopyFromParserProperties(true, false, ',', 0),
            List.of()
        );

        assertThat(lineProcessor.moveNext()).isTrue();
        assertThat(lineProcessor.currentElement().get(0)).isEqualTo("{\"x\": 10}");
        assertThat(lineProcessor.currentElement().get(1)).isEqualTo(10);
    }

    @Test
    public void test_line_processor_parses_csv_input_with_header() throws Exception {
        URI uri = new URI("file:///dummy.txt");
        BatchIterator<LineCursor> source = InMemoryBatchIterator.of(
            List.of(
                new LineCursor(uri, 1, "x,y", null),
                new LineCursor(uri, 2, "10,20", null)
            ),
            null,
            false
        );
        Context<LineCollectorExpression<?>> ctxForRefs = inputFactory.ctxForRefs(
            CoordinatorTxnCtx.systemTransactionContext(),
            FileLineReferenceResolver::getImplementation
        );
        ctxForRefs.add(List.of(
            TestingHelpers.createReference(DocSysColumns.RAW, DataTypes.STRING),
            TestingHelpers.createReference("x", DataTypes.INTEGER),
            TestingHelpers.createReference("y", DataTypes.INTEGER)
        ));

        LineProcessor lineProcessor = new LineProcessor(
            source,
            ctxForRefs.topLevelInputs(),
            ctxForRefs.expressions(),
            InputFormat.CSV,
            new CopyFromParserProperties(true, true, ',', 0),
            List.of()
        );

        assertThat(lineProcessor.moveNext()).isTrue();
        assertThat(lineProcessor.currentElement().get(0)).isEqualTo("{\"x\":\"10\",\"y\":\"20\"}");
        assertThat(lineProcessor.currentElement().get(1)).isEqualTo(10);
        assertThat(lineProcessor.currentElement().get(2)).isEqualTo(20);
    }

    @Test
    public void test_line_processor_parses_csv_input_without_header() throws Exception {
        URI uri = new URI("file:///dummy.txt");
        BatchIterator<LineCursor> source = InMemoryBatchIterator.of(
            List.of(
                new LineCursor(uri, 1, "1,2", null),
                new LineCursor(uri, 2, "10,20", null)
            ),
            null,
            false
        );
        Context<LineCollectorExpression<?>> ctxForRefs = inputFactory.ctxForRefs(
            CoordinatorTxnCtx.systemTransactionContext(),
            FileLineReferenceResolver::getImplementation
        );
        ctxForRefs.add(List.of(
            TestingHelpers.createReference(DocSysColumns.RAW, DataTypes.STRING),
            TestingHelpers.createReference("x", DataTypes.INTEGER),
            TestingHelpers.createReference("y", DataTypes.INTEGER)
        ));

        LineProcessor lineProcessor = new LineProcessor(
            source,
            ctxForRefs.topLevelInputs(),
            ctxForRefs.expressions(),
            InputFormat.CSV,
            new CopyFromParserProperties(true, false, ',', 0),
            List.of("x", "y")
        );
        assertThat(lineProcessor.moveNext()).isTrue();
        assertThat(lineProcessor.currentElement().get(0)).isEqualTo("{\"x\":\"1\",\"y\":\"2\"}");
        assertThat(lineProcessor.currentElement().get(1)).isEqualTo(1);
        assertThat(lineProcessor.currentElement().get(2)).isEqualTo(2);
    }

    @Test
    public void test_line_processor_sets_parse_failure_on_invalid_inputs() throws Exception {
        URI uri = new URI("file:///dummy.txt");
        BatchIterator<LineCursor> source = InMemoryBatchIterator.of(
            List.of(
                new LineCursor(uri, 1, "x,y", null),
                new LineCursor(uri, 2, "1,2,3,4", null)
            ),
            null,
            false
        );
        Context<LineCollectorExpression<?>> ctxForRefs = inputFactory.ctxForRefs(
            CoordinatorTxnCtx.systemTransactionContext(),
            FileLineReferenceResolver::getImplementation
        );
        ctxForRefs.add(List.of(
            TestingHelpers.createReference(DocSysColumns.RAW, DataTypes.STRING),
            TestingHelpers.createReference(SourceParsingFailureExpression.COLUMN_NAME, DataTypes.STRING),
            TestingHelpers.createReference("x", DataTypes.INTEGER)
        ));

        LineProcessor lineProcessor = new LineProcessor(
            source,
            ctxForRefs.topLevelInputs(),
            ctxForRefs.expressions(),
            InputFormat.CSV,
            new CopyFromParserProperties(true, true, ',', 0),
            List.of()
        );

        assertThat(lineProcessor.moveNext()).isTrue();
        assertThat(lineProcessor.currentElement().get(0)).isNull();
        assertThat(lineProcessor.currentElement().get(1)).isEqualTo("Number of values exceeds number of keys in csv file at line 2");
        assertThat(lineProcessor.currentElement().get(2)).isNull();
    }
}
