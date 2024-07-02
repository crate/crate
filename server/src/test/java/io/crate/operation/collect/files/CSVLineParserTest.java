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

package io.crate.operation.collect.files;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import io.crate.analyze.CopyFromParserProperties;

public class CSVLineParserTest {

    private CSVLineParser csvParser;
    private byte[] result;

    @Before
    public void setup() {
        csvParser = new CSVLineParser(CopyFromParserProperties.DEFAULT, List.of("Code", "Country", "City"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void parse_givenEmptyHeader_thenThrowsException() throws IOException {
        String header = "\n";
        csvParser.parseHeader(header);
        csvParser.parse("GER,Germany\n", 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void parse_givenDuplicateKey_thenThrowsException() throws IOException {
        String header = "Code,Country,Country\n";
        csvParser.parseHeader(header);
        result = csvParser.parse("GER,Germany,Another\n", 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void parse_givenMissingKey_thenThrowsException() throws IOException {
        String header = "Code,\n";
        csvParser.parseHeader(header);
        result = csvParser.parse("GER,Germany\n", 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void parse_givenExtraValue_thenIgnoresTheKeyWithoutValue() throws IOException {
        String header = "Code,Country\n";
        csvParser.parseHeader(header);
        csvParser.parse("GER,Germany,Berlin\n", 0);
    }

    @Test
    public void parse_givenExtraKey_thenIgnoresTheKeyWithoutValue() throws IOException {
        String header = "Code,Country,Another\n";
        csvParser.parseHeader(header);
        result = csvParser.parse("GER,Germany\n", 0);

        assertThat(new String(result, StandardCharsets.UTF_8)).isEqualTo("{\"Code\":\"GER\",\"Country\":\"Germany\"}");
    }

    @Test
    public void parse_givenCSVInput_thenParsesToMap() throws IOException {
        String header = "Code,Country\n";
        csvParser.parseHeader(header);
        result = csvParser.parse("GER,Germany\n", 0);

        assertThat(new String(result, StandardCharsets.UTF_8)).isEqualTo("{\"Code\":\"GER\",\"Country\":\"Germany\"}");
    }

    @Test
    public void parse_givenEmptyRow_thenParsesToEmptyJson() throws IOException {
        String header = "Code,Country\n";
        csvParser.parseHeader(header);
        result = csvParser.parse("", 0);

        assertThat(result).isEqualTo("{}".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void parse_givenEmptyRowWithCommas_thenParsesAsEmptyStrings() throws IOException {
        String header = "Code,Country\n";
        csvParser.parseHeader(header);
        result = csvParser.parse(",", 0);

        assertThat(new String(result, StandardCharsets.UTF_8)).isEqualTo("{\"Code\":\"\",\"Country\":\"\"}");
    }

    @Test
    public void parse_givenEscapedComma_thenParsesLineCorrectly() throws IOException {
        String header = "Code,\"Coun, try\"\n";
        csvParser = new CSVLineParser(
            new CopyFromParserProperties(true, true, CsvSchema.DEFAULT_COLUMN_SEPARATOR, 0),
            List.of("Code", "Coun, try", "City"));
        csvParser.parseHeader(header);
        result = csvParser.parse("GER,Germany\n", 0);

        assertThat(new String(result, StandardCharsets.UTF_8)).isEqualTo("{\"Code\":\"GER\",\"Coun, try\":\"Germany\"}");
    }

    @Test
    public void test_quoted_and_unquoted_empty_string_converted_to_null_empty_string_as_null_is_set() throws IOException {
        String header = "Code,Country,City\n";
        csvParser = new CSVLineParser(
            new CopyFromParserProperties(true, true, CsvSchema.DEFAULT_COLUMN_SEPARATOR, 0),
                                          List.of("Code", "Country", "City"));
        csvParser.parseHeader(header);
        result = csvParser.parse("GER,,\"\"\n", 0);

        assertThat(new String(result, StandardCharsets.UTF_8)).isEqualTo("{\"Code\":\"GER\",\"Country\":null,\"City\":null}");
    }

    @Test
    public void test_parse_csv_with_configured_delimiter_parses_lines_correctly() throws IOException {
        String header = "Code|Country|City\n";
        csvParser = new CSVLineParser(
            new CopyFromParserProperties(true, true, '|', 0),
                                          List.of("Code", "Country", "City"));
        csvParser.parseHeader(header);
        result = csvParser.parse("GER|Germany|Berlin\n", 0);

        assertThat(new String(result, StandardCharsets.UTF_8)).isEqualTo("{\"Code\":\"GER\",\"Country\":\"Germany\",\"City\":\"Berlin\"}");
    }

    @Test
    public void parse_givenRowWithMissingValue_thenTheValueIsAssignedToKeyAsAnEmptyString() throws IOException {
        String header = "Code,Country,City\n";
        csvParser.parseHeader(header);
        result = csvParser.parse("GER,,Berlin\n", 0);

        assertThat(new String(result, StandardCharsets.UTF_8)).isEqualTo("{\"Code\":\"GER\",\"Country\":\"\",\"City\":\"Berlin\"}");
    }

    @Test
    public void parse_givenTrailingWhiteSpaceInHeader_thenParsesToMapWithoutWhitespace() throws IOException {
        String header = "Code ,Country  \n";
        csvParser.parseHeader(header);
        result = csvParser.parse("GER,Germany\n", 0);

        assertThat(new String(result, StandardCharsets.UTF_8)).isEqualTo("{\"Code\":\"GER\",\"Country\":\"Germany\"}");
    }

    @Test
    public void parse_givenTrailingWhiteSpaceInRow_thenParsesToMapWithoutWhitespace() throws IOException {
        String header = "Code ,Country\n";
        csvParser.parseHeader(header);
        result = csvParser.parse("GER        ,Germany\n", 0);

        assertThat(new String(result, StandardCharsets.UTF_8)).isEqualTo("{\"Code\":\"GER\",\"Country\":\"Germany\"}");
    }

    @Test
    public void parse_givenPrecedingWhiteSpaceInHeader_thenParsesToMapWithoutWhitespace() throws IOException {
        String header = "         Code,         Country\n";
        csvParser.parseHeader(header);
        result = csvParser.parse("GER,Germany\n", 0);

        assertThat(new String(result, StandardCharsets.UTF_8)).isEqualTo("{\"Code\":\"GER\",\"Country\":\"Germany\"}");
    }

    @Test
    public void parse_givenPrecedingWhiteSpaceInRow_thenParsesToMapWithoutWhitespace() throws IOException {
        String header = "Code,Country\n";
        csvParser.parseHeader(header);
        result = csvParser.parse("GER,               Germany\n", 0);

        assertThat(new String(result, StandardCharsets.UTF_8)).isEqualTo("{\"Code\":\"GER\",\"Country\":\"Germany\"}");
    }

    @Test
    public void parse_targetColumnsMoreThanCsvHeader_thenKeepOnlyTargetValues() throws IOException {
        String header = "Code,Country\n";
        csvParser = new CSVLineParser(
            new CopyFromParserProperties(true, true,CsvSchema.DEFAULT_COLUMN_SEPARATOR, 0),
            List.of("Code", "Country", "City"));

        csvParser.parseHeader(header);
        result = csvParser.parse("GER,Germany\n", 0);

        assertThat(new String(result, StandardCharsets.UTF_8)).isEqualTo("{\"Code\":\"GER\",\"Country\":\"Germany\"}");
    }

    @Test
    public void parse_targetColumnsLessThanCsvHeader_thenDropExtraCsvValues() throws IOException {
        String header = "Code,Country,City\n";
        csvParser = new CSVLineParser(
            new CopyFromParserProperties(true, true,CsvSchema.DEFAULT_COLUMN_SEPARATOR, 0),
                                          List.of("Code", "Country"));
        csvParser.parseHeader(header);
        result = csvParser.parse("GER,Germany,Berlin\n", 0);

        assertThat(new String(result, StandardCharsets.UTF_8)).isEqualTo("{\"Code\":\"GER\",\"Country\":\"Germany\"}");
    }

    @Test
    public void parse_targetColumnsSameCsvValuesNoHeader_thenParseAsIs() throws IOException {
        csvParser = new CSVLineParser(
            new CopyFromParserProperties(true, false, CsvSchema.DEFAULT_COLUMN_SEPARATOR, 0),
            List.of("Code", "Country", "City"));
        csvParser.parseWithoutHeader("GER,Germany,Berlin\n", 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void parse_targetColumnsMoreThanCsvValuesNoHeader_thenThrowException() throws IOException {
        csvParser = new CSVLineParser(
            new CopyFromParserProperties(true, false, CsvSchema.DEFAULT_COLUMN_SEPARATOR, 0),
            List.of("Code", "Country", "City"));
        csvParser.parseWithoutHeader("GER,Germany\n", 0);
    }

    @Test
    public void parse_targetColumnsLessThanCsvValuesNoHeader_thenDropExtraCsvValues() throws IOException {
        csvParser = new CSVLineParser(
            new CopyFromParserProperties(true, false, CsvSchema.DEFAULT_COLUMN_SEPARATOR, 0),
            List.of("Code", "Country"));
        result = csvParser.parseWithoutHeader("GER,Germany,Berlin\n", 0);
        assertThat(new String(result, StandardCharsets.UTF_8)).isEqualTo("{\"Code\":\"GER\",\"Country\":\"Germany\"}");
    }

    @Test
    public void parse_targetColumnsNotInOrder_thenParseWithOrder() throws IOException {
        String header = "Code,Country,City\n";
        csvParser = new CSVLineParser(
            new CopyFromParserProperties(true, true, CsvSchema.DEFAULT_COLUMN_SEPARATOR, 0),
            List.of("City", "Code"));
        csvParser.parseHeader(header);
        result = csvParser.parse("GER,Germany,Berlin\n", 0);
        assertThat(new String(result, StandardCharsets.UTF_8)).isEqualTo("{\"Code\":\"GER\",\"City\":\"Berlin\"}");
    }

    @Test
    public void parse_targetColumnsEmpty_thenParseFromHeader() throws IOException {
        String header = "Code,Country,City\n";
        csvParser = new CSVLineParser(
            new CopyFromParserProperties(true, true, CsvSchema.DEFAULT_COLUMN_SEPARATOR, 0),
            List.of());
        csvParser.parseHeader(header);
        result = csvParser.parse("GER,Germany,Berlin\n", 0);
        assertThat(new String(result, StandardCharsets.UTF_8)).isEqualTo("{\"Code\":\"GER\",\"Country\":\"Germany\",\"City\":\"Berlin\"}");
    }
}
