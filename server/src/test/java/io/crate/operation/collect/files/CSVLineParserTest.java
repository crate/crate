package io.crate.operation.collect.files;

import io.crate.analyze.CopyFromParserProperties;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class CSVLineParserTest {

    private CSVLineParser csvParser;
    private byte[] result;

    @Before
    public void setup() {
        csvParser = new CSVLineParser(CopyFromParserProperties.DEFAULT);
    }

    @Test(expected = IllegalArgumentException.class)
    public void parse_givenEmptyHeader_thenThrowsException() throws IOException {
        String header = "\n";
        csvParser.parseHeader(header);
        csvParser.parse("GER,Germany\n");
    }

    @Test(expected = IllegalArgumentException.class)
    public void parse_givenDuplicateKey_thenThrowsException() throws IOException {
        String header = "Code,Country,Country\n";
        csvParser.parseHeader(header);
        result = csvParser.parse("GER,Germany,Another\n");
    }

    @Test(expected = IllegalArgumentException.class)
    public void parse_givenMissingKey_thenThrowsException() throws IOException {
        String header = "Code,\n";
        csvParser.parseHeader(header);
        result = csvParser.parse("GER,Germany\n");
    }

    @Test(expected = IllegalArgumentException.class)
    public void parse_givenExtraValue_thenIgnoresTheKeyWithoutValue() throws IOException {
        String header = "Code,Country\n";
        csvParser.parseHeader(header);
        csvParser.parse("GER,Germany,Berlin\n");
    }

    @Test
    public void parse_givenExtraKey_thenIgnoresTheKeyWithoutValue() throws IOException {
        String header = "Code,Country,Another\n";
        csvParser.parseHeader(header);
        result = csvParser.parse("GER,Germany\n");

        assertThat(new String(result, StandardCharsets.UTF_8), is("{\"Code\":\"GER\",\"Country\":\"Germany\"}"));
    }

    @Test
    public void parse_givenCSVInput_thenParsesToMap() throws IOException {
        String header = "Code,Country\n";
        csvParser.parseHeader(header);
        result = csvParser.parse("GER,Germany\n");

        assertThat(new String(result, StandardCharsets.UTF_8), is("{\"Code\":\"GER\",\"Country\":\"Germany\"}"));
    }

    @Test
    public void parse_givenEmptyRow_thenParsesToEmptyJson() throws IOException {
        String header = "Code,Country\n";
        csvParser.parseHeader(header);
        result = csvParser.parse("");

        assertThat(result, is("{}".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void parse_givenEmptyRowWithCommas_thenParsesAsEmptyStrings() throws IOException {
        String header ="Code,Country\n";
        csvParser.parseHeader(header);
        result = csvParser.parse(",");

        assertThat(new String(result, StandardCharsets.UTF_8), is("{\"Code\":\"\",\"Country\":\"\"}"));
    }

    @Test
    public void parse_givenEscapedComma_thenParsesLineCorrectly() throws IOException {
        String header = "Code,\"Coun, try\"\n";
        csvParser.parseHeader(header);
        result = csvParser.parse("GER,Germany\n");

        assertThat(new String(result, StandardCharsets.UTF_8), is("{\"Code\":\"GER\",\"Coun, try\":\"Germany\"}"));
    }

    @Test
    public void test_quoted_and_unquoted_empty_string_converted_to_null_empty_string_as_null_is_set() throws IOException {
        String header = "Code,Country,City\n";
        csvParser = new CSVLineParser(new CopyFromParserProperties(true));
        csvParser.parseHeader(header);
        result = csvParser.parse("GER,,\"\"\n");

        assertThat(
            new String(result, StandardCharsets.UTF_8),
            is("{\"Code\":\"GER\",\"Country\":null,\"City\":null}")
        );
    }

    @Test
    public void parse_givenRowWithMissingValue_thenTheValueIsAssignedToKeyAsAnEmptyString() throws IOException {
        String header = "Code,Country,City\n";
        csvParser.parseHeader(header);
        result = csvParser.parse("GER,,Berlin\n");

        assertThat(new String(result, StandardCharsets.UTF_8), is("{\"Code\":\"GER\",\"Country\":\"\",\"City\":\"Berlin\"}"));
    }

    @Test
    public void parse_givenTrailingWhiteSpaceInHeader_thenParsesToMapWithoutWhitespace() throws IOException {
        String header = "Code ,Country  \n";
        csvParser.parseHeader(header);
        result = csvParser.parse("GER,Germany\n");

        assertThat(new String(result, StandardCharsets.UTF_8), is("{\"Code\":\"GER\",\"Country\":\"Germany\"}"));
    }

    @Test
    public void parse_givenTrailingWhiteSpaceInRow_thenParsesToMapWithoutWhitespace() throws IOException {
        String header = "Code ,Country\n";
        csvParser.parseHeader(header);
        result = csvParser.parse("GER        ,Germany\n");

        assertThat(new String(result, StandardCharsets.UTF_8), is("{\"Code\":\"GER\",\"Country\":\"Germany\"}"));
    }

    @Test
    public void parse_givenPrecedingWhiteSpaceInHeader_thenParsesToMapWithoutWhitespace() throws IOException {
        String header = "         Code,         Country\n";
        csvParser.parseHeader(header);
        result = csvParser.parse("GER,Germany\n");

        assertThat(new String(result, StandardCharsets.UTF_8), is("{\"Code\":\"GER\",\"Country\":\"Germany\"}"));
    }

    @Test
    public void parse_givenPrecedingWhiteSpaceInRow_thenParsesToMapWithoutWhitespace() throws IOException {
        String header = "Code,Country\n";
        csvParser.parseHeader(header);
        result = csvParser.parse("GER,               Germany\n");

        assertThat(new String(result, StandardCharsets.UTF_8), is("{\"Code\":\"GER\",\"Country\":\"Germany\"}"));
    }
}
