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
import io.crate.blob.v2.BlobShard;
import io.crate.operation.Input;
import io.crate.operation.collect.blobs.BlobCollectorExpression;
import io.crate.operation.collect.blobs.BlobDocCollector;
import io.crate.operation.projectors.CollectingProjector;
import io.crate.operation.reference.doc.blob.BlobDigestExpression;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.FileSystemUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BlobDocCollectorTest {

    private Path tmpDir;

    @Before
    public void prepare() throws Exception {
        tmpDir = Files.createTempDirectory(getClass().getName());
    }

    @After
    public void cleanUp() throws Exception {
        if (tmpDir != null) {
            FileSystemUtils.deleteRecursively(tmpDir.toFile());
        }
    }

    @Test
    public void testBlobFound() throws Exception {
        BlobContainer container = new BlobContainer(tmpDir.toFile());
        String digest = "417de3231e23dcd6d224ff60918024bc6c59aa58";

        File blob = new File(container.getVarDirectory().getAbsolutePath() + "/01/" + digest);
        blob.createNewFile();

        BlobDigestExpression expression = new BlobDigestExpression();
        Input<Boolean> condition = new Input<Boolean>() {
            @Override
            public Boolean value() {
                return true;
            }
        };

        CollectingProjector projector = getProjector(
                container,
                Arrays.<Input<?>>asList(expression),
                Arrays.<BlobCollectorExpression<?>>asList(expression),
                condition
        );
        Object[][] result = projector.result().get();

        assertEquals(digest, ((BytesRef)result[0][0]).utf8ToString());
    }

    private CollectingProjector getProjector(BlobContainer container,
                                             List<Input<?>> inputs,
                                             List<BlobCollectorExpression<?>> expressions,
                                             Input<Boolean> condition) throws Exception {
        CollectingProjector projector = new CollectingProjector();
        BlobShard blobShard = mock(BlobShard.class);
        when(blobShard.blobContainer()).thenReturn(container);

        BlobDocCollector collector = new BlobDocCollector(
                blobShard,
                inputs,
                expressions,
                condition,
                projector
        );

        projector.startProjection();
        collector.doCollect();
        return projector;
    }

}
