///*
// * Licensed to Crate.io GmbH ("Crate") under one or more contributor
// * license agreements.  See the NOTICE file distributed with this work for
// * additional information regarding copyright ownership.  Crate licenses
// * this file to you under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.  You may
// * obtain a copy of the License at
// *
// *   http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
// * License for the specific language governing permissions and limitations
// * under the License.
// *
// * However, if you have executed another commercial license agreement
// * with Crate these terms will supersede the license and you may use the
// * software solely pursuant to the terms of the relevant commercial agreement.
// */
//
//package io.crate.execution.engine.collect.files;
//
//import static io.crate.execution.dsl.phases.FileUriCollectPhase.InputFormat.CSV;
//import static io.crate.execution.dsl.phases.FileUriCollectPhase.InputFormat.JSON;
//import static org.hamcrest.MatcherAssert.assertThat;
//import static org.hamcrest.Matchers.is;
//import static org.hamcrest.Matchers.nullValue;
//
//import java.io.BufferedReader;
//import java.io.IOException;
//import java.io.Reader;
//import java.io.StringReader;
//import java.net.URI;
//import java.net.URISyntaxException;
//import java.util.List;
//
//import org.junit.Before;
//import org.junit.Test;
//
//import io.crate.analyze.CopyFromParserProperties;
//
//public class LineProcessorTest {
//
//    private LineProcessor subjectUnderTest;
//    private URI uri;
//    private BufferedReader bufferedReader;
//
//    @Before
//    public void setup() {
//        subjectUnderTest = new LineProcessor(CopyFromParserProperties.DEFAULT, List.of("a", "b"));
//    }
//
//    @Test
//    public void readFirstLine_givenFileExtensionIsCsv_AndDefaultJSONFileFormat_thenReadsLine() throws URISyntaxException, IOException {
//        uri = new URI("file.csv");
//        Reader reader = new StringReader("some/string");
//        bufferedReader = new BufferedReader(reader);
//
//        subjectUnderTest.readFirstLine(uri, JSON);
//
//        assertThat(bufferedReader.readLine(), is(nullValue()));
//    }
//
//    @Test
//    public void readFirstLine_givenFileFormatIsCsv_thenReadsLine() throws URISyntaxException, IOException {
//        uri = new URI("file.any");
//        Reader reader = new StringReader("some/string");
//        bufferedReader = new BufferedReader(reader);
//
//        subjectUnderTest.readFirstLine(uri, CSV);
//
//        assertThat(bufferedReader.readLine(), is(nullValue()));
//    }
//
//    @Test
//    public void readFirstLine_givenFileExtensionIsJson__AndDefaultJSONFileFormat_thenDoesNotReadLine() throws URISyntaxException, IOException {
//        uri = new URI("file.json");
//        Reader reader = new StringReader("some/string");
//        bufferedReader = new BufferedReader(reader);
//
//        subjectUnderTest.readFirstLine(uri, JSON);
//
//        assertThat(bufferedReader.readLine(), is("some/string"));
//    }
//
//    @Test
//    public void readFirstLine_givenFileFormatIsJson_thenDoesNotReadLine() throws URISyntaxException, IOException {
//        uri = new URI("file.any");
//        Reader reader = new StringReader("some/string");
//        bufferedReader = new BufferedReader(reader);
//
//        subjectUnderTest.readFirstLine(uri, JSON);
//
//        assertThat(bufferedReader.readLine(), is("some/string"));
//    }
//}
