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

import io.crate.operation.Input;
import io.crate.operation.collect.CrateCollector;
import io.crate.operation.projectors.Projector;
import org.apache.lucene.search.CollectionTerminatedException;

import java.io.*;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class FileReadingCollector implements CrateCollector {

    private final String fileUri;
    private final Projector downstream;
    private final boolean compressed;
    private final List<Input<?>> inputs;
    private final List<LineCollectorExpression<?>> collectorExpressions;

    public enum FileFormat {
        JSON
    }

    public FileReadingCollector(String fileUri,
                                List<Input<?>> inputs,
                                List<LineCollectorExpression<?>> collectorExpressions,
                                Projector downstream,
                                FileFormat format,
                                boolean compressed) {
        this.fileUri = fileUri;
        this.downstream = downstream;
        this.compressed = compressed;
        this.inputs = inputs;
        this.collectorExpressions = collectorExpressions;
    }

    @Override
    public void doCollect() throws IOException, CollectionTerminatedException {
        BufferedReader reader;
        if (compressed) {
            reader = new BufferedReader(
                    new InputStreamReader(new GZIPInputStream(new FileInputStream(fileUri))));
        } else {
            reader = new BufferedReader(new FileReader(fileUri));
        }

        CollectorContext collectorContext = new CollectorContext();
        for (LineCollectorExpression<?> collectorExpression : collectorExpressions) {
            collectorExpression.startCollect(collectorContext);
        }
        Object[] newRow;
        String line;
        try {
            while ((line = reader.readLine()) != null) {
                collectorContext.lineContext().rawSource(line.getBytes());
                newRow = new Object[inputs.size()];
                for (LineCollectorExpression expression : collectorExpressions) {
                    expression.setNextLine(line);
                }
                int i = 0;
                for (Input<?> input : inputs) {
                    newRow[i++] = input.value();
                }
                if (!downstream.setNextRow(newRow)) {
                    throw new CollectionTerminatedException();
                }
            }
        } finally {
            reader.close();
        }
    }
}
