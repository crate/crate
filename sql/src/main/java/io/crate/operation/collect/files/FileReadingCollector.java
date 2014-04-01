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

import javax.annotation.Nullable;
import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

public class FileReadingCollector implements CrateCollector {

    private final Path filePath;
    private final String nodePath;
    private Projector downstream;
    private final boolean compressed;
    private final List<Input<?>> inputs;
    private final List<LineCollectorExpression<?>> collectorExpressions;

    public enum FileFormat {
        JSON
    }

    public FileReadingCollector(String filePath,
                                List<Input<?>> inputs,
                                List<LineCollectorExpression<?>> collectorExpressions,
                                Projector downstream,
                                FileFormat format,
                                boolean compressed,
                                @Nullable String nodePath) {
        this.filePath = Paths.get(filePath);
        this.nodePath = nodePath;
        downstream(downstream);
        this.compressed = compressed;
        this.inputs = inputs;
        this.collectorExpressions = collectorExpressions;
    }

    @Nullable
    private BufferedReader getReader() throws IOException {
        File file;
        if (!filePath.isAbsolute() && nodePath != null) {
            file = new File(nodePath, filePath.toString()).getAbsoluteFile();
        } else {
            file = new File(filePath.toUri());
        }
        if (file.isDirectory()) {
            return readerFromDirectory(file);
        } else if (file.exists()) {
            if (compressed) {
                return new BufferedReader(
                        new InputStreamReader(new GZIPInputStream(new FileInputStream(file))));
            } else {
                return new BufferedReader(new InputStreamReader(new FileInputStream(file)));
            }
        } else if (!file.exists()) {
            return readerFromRegexUri(file);
        }
        return null;
    }

    @Override
    public void doCollect() throws IOException, CollectionTerminatedException {
        BufferedReader reader = getReader();
        if (reader == null) {
            if (downstream != null) {
                downstream.upstreamFinished();
            }
            return;
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
            downstream.upstreamFinished();
            reader.close();
        }
    }

    private BufferedReader readerFromRegexUri(File file) throws IOException {
        File parent = file.getParentFile();
        if (!parent.isDirectory()) {
            return null;
        }
        File[] files = parent.listFiles();
        if (files == null) {
            return null;
        }
        Pattern pattern = Pattern.compile(file.getName());
        List<InputStream> inputStreams = new ArrayList<>(files.length);
        for (File fi : files) {
            if (pattern.matcher(fi.getName()).matches()) {
                try {
                    if (compressed) {
                        inputStreams.add(new GZIPInputStream(new FileInputStream(fi)));
                    } else {
                        inputStreams.add(new FileInputStream(fi));
                    }
                } catch (FileNotFoundException e) {
                    // ignore - race condition? file got deleted just now.
                }
            }
        }
        return new BufferedReader(
                new InputStreamReader(new SequenceInputStream(Collections.enumeration(inputStreams))));
    }

    private BufferedReader readerFromDirectory(File file) throws IOException {
        File[] files = file.listFiles();
        if (files == null) {
            return null;
        }
        List<InputStream> inputStreams = new ArrayList<>(files.length);
        for (File fi : files) {
            try {
                if (compressed) {
                    inputStreams.add(new GZIPInputStream(new FileInputStream(fi)));
                } else {
                    inputStreams.add(new FileInputStream(fi));
                }
            } catch (FileNotFoundException e) {
                // ignore - race condition? file got deleted just now.
            }
        }
        return new BufferedReader(
                new InputStreamReader(new SequenceInputStream(Collections.enumeration(inputStreams))));
    }

    @Override
    public void downstream(Projector downstream) {
        this.downstream = downstream;
        downstream.registerUpstream(this);
    }

    @Override
    public Projector downstream() {
        return this.downstream;
    }
}
