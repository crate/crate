/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import io.crate.blob.BlobContainer;
import io.crate.core.collections.Bucket;
import io.crate.jobs.ExecutionState;
import io.crate.operation.Input;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.collect.blobs.BlobDocCollector;
import io.crate.operation.reference.doc.blob.BlobDigestExpression;
import io.crate.operation.reference.doc.blob.BlobLastModifiedExpression;
import io.crate.planner.symbol.Literal;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.CollectingRowReceiver;
import org.apache.lucene.util.BytesRef;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static io.crate.testing.TestingHelpers.isRow;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;

public class BlobDocCollectorTest extends CrateUnitTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();
    private BlobDigestExpression digestExpression = new BlobDigestExpression();
    private BlobLastModifiedExpression ctimeExpression = new BlobLastModifiedExpression();

    @Test
    public void testBlobFound() throws Exception {
        BlobContainer container = new BlobContainer(tempFolder.newFolder());
        String digest = "417de3231e23dcd6d224ff60918024bc6c59aa58";
        long mtime = createFile(container, digest).lastModified();

        Input<Boolean> condition = Literal.BOOLEAN_TRUE;
        CollectingRowReceiver projector = getProjector(
                container,
                Arrays.<Input<?>>asList(digestExpression, ctimeExpression),
                Arrays.<CollectExpression<File, ?>>asList(digestExpression, ctimeExpression),
                condition
        );
        Bucket result = projector.result();
        assertThat(result, contains(isRow(new BytesRef(digest), mtime)));
    }

    @Test
    public void testConditionThatReturnsNull() throws Exception {
        BlobContainer container = new BlobContainer(tempFolder.newFolder());
        createFile(container, "417de3231e23dcd6d224ff60918024bc6c59aa58");

        Input<Boolean> condition = Literal.<Boolean>newLiteral((Boolean) null);
        CollectingRowReceiver projector = getProjector(
                container,
                Arrays.<Input<?>>asList(digestExpression, ctimeExpression),
                Arrays.<CollectExpression<File, ?>>asList(digestExpression, ctimeExpression),
                condition
        );
        Bucket result = projector.result();
        assertThat(result.size(), is(0));
    }

    private File createFile(BlobContainer container, String digest) throws IOException {
        File blob = new File(container.getVarDirectory().getAbsolutePath() + "/01/" + digest);
        assertTrue(blob.createNewFile());
        return blob;
    }

    private CollectingRowReceiver getProjector(BlobContainer container,
                                             List<Input<?>> inputs,
                                             List<CollectExpression<File, ?>> expressions,
                                             Input<Boolean> condition) throws Exception {
        CollectingRowReceiver projector = new CollectingRowReceiver();

        BlobDocCollector collector = new BlobDocCollector(
                container,
                inputs,
                expressions,
                condition,
                projector
        );

        projector.prepare(mock(ExecutionState.class));
        collector.doCollect();
        return projector;
    }
}
