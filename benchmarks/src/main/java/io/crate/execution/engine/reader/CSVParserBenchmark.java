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

package io.crate.execution.engine.reader;


import io.crate.analyze.CopyFromParserProperties;
import io.crate.operation.collect.files.CSVLineParser;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;


@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
public class CSVParserBenchmark {

    File tempFile;
    private CSVLineParser streamParser;
    private CSVLineParser lineParser;
    BufferedReader reader;

    @Setup
    public void init() throws IOException {
        tempFile = File.createTempFile("temp", null);
        try (OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(tempFile), StandardCharsets.UTF_8)) {
           // writer.write("name,id\n");
            writer.write("Arthur,4\n");
            writer.write("Trillian,5\n");
            writer.write("Emma,5\n");
            writer.write("Emily,9\n");
            writer.write("Sarah,5\n");
            writer.write("John,5\n");
            writer.write("Mical,9\n");
            writer.write("Mary,5\n");
            writer.write("Jimmy,9\n");
            writer.write("Tom,5\n");
            writer.write("Neil,0\n");
            writer.write("Rose,5\n");
            writer.write("Gobnait,5\n");
            writer.write("Rory,1\n");
            writer.write("Martin,11\n");
            writer.write("Arthur,4\n");
            writer.write("Trillian,5\n");
            writer.write("Emma,5\n");
            writer.write("Emily,9\n");
            writer.write("Sarah,5\n");
            writer.write("John,5\n");
            writer.write("Mical,9\n");
            writer.write("Mary,5\n");
            writer.write("Jimmy,9\n");
            writer.write("Tom,5\n");
            writer.write("Neil,0\n");
            writer.write("Rose,5\n");
            writer.write("Gobnait,5\n");
            writer.write("Rory,1\n");
            writer.write("Martin,11\n");
        }

        // header logic is not yet addressed for streamParser, make it false for the simplicity of the benchmark

        lineParser = new CSVLineParser(
            new CopyFromParserProperties(false, false, ','),
            List.of("id", "name")
        );
        reader = new BufferedReader(new InputStreamReader(new FileInputStream(tempFile), StandardCharsets.UTF_8));

        streamParser = new CSVLineParser(
            new CopyFromParserProperties(false, false, ','),
            List.of("id", "name"),
            new BufferedInputStream(new FileInputStream(tempFile))
        );

    }

    @Benchmark()
    public void measure_csv_parser_direct_input_stream(Blackhole blackhole) throws IOException {
        byte[] parsedBytes = null;
        while ((parsedBytes = streamParser.parse()) != null) {
            //System.out.println("ParsedLine direct=" + new String(parsedBytes, StandardCharsets.UTF_8));
            blackhole.consume(parsedBytes);
        }
    }

    @Benchmark()
    public void measure_csv_parser_line_by_line(Blackhole blackhole) throws IOException {
        String line = null;
        int row = 0;
        while ((line = reader.readLine()) != null) {
            byte[] parsedBytes = lineParser.parseWithoutHeader(line, row);
            blackhole.consume(parsedBytes);
            //System.out.println("line="+line+"; parsed line by line="+new String(parsedBytes, StandardCharsets.UTF_8));
            row++;
        }
    }

    @TearDown
    public void cleanup() throws IOException {
        reader.close();
        streamParser.close();
        tempFile.delete();
    }
}
