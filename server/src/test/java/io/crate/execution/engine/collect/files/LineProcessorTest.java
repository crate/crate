package io.crate.execution.engine.collect.files;

import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;

import static io.crate.execution.dsl.phases.FileUriCollectPhase.InputFormat.CSV;
import static io.crate.execution.dsl.phases.FileUriCollectPhase.InputFormat.JSON;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.*;

public class LineProcessorTest {

    private LineProcessor subjectUnderTest;
    private URI uri;
    private BufferedReader bufferedReader;

    @Before
    public void setup() {
        subjectUnderTest  = new LineProcessor();
    }

    @Test
    public void readFirstLine_givenFileExtensionIsCsv_AndDefaultJSONFileFormat_thenReadsLine() throws URISyntaxException, IOException {
        uri = new URI ("file.csv");
        Reader reader = new StringReader("some/string");
        bufferedReader = new BufferedReader(reader);

        subjectUnderTest.readFirstLine(uri, JSON, bufferedReader);

        assertThat(bufferedReader.readLine(), is(nullValue()));;
    }

    @Test
    public void readFirstLine_givenFileFormatIsCsv_thenReadsLine() throws URISyntaxException, IOException {
        uri = new URI ("file.any");
        Reader reader = new StringReader("some/string");
        bufferedReader = new BufferedReader(reader);

        subjectUnderTest.readFirstLine(uri, CSV, bufferedReader);

        assertThat(bufferedReader.readLine(), is(nullValue()));;
    }

    @Test
    public void readFirstLine_givenFileExtensionIsJson__AndDefaultJSONFileFormat_thenDoesNotReadLine() throws URISyntaxException, IOException {
        uri = new URI ("file.json");
        Reader reader = new StringReader("some/string");
        bufferedReader = new BufferedReader(reader);

        subjectUnderTest.readFirstLine(uri, JSON, bufferedReader);

        assertThat(bufferedReader.readLine(), is("some/string"));
    }

    @Test
    public void readFirstLine_givenFileFormatIsJson_thenDoesNotReadLine() throws URISyntaxException, IOException {
        uri = new URI ("file.any");
        Reader reader = new StringReader("some/string");
        bufferedReader = new BufferedReader(reader);

        subjectUnderTest.readFirstLine(uri, JSON, bufferedReader);

        assertThat(bufferedReader.readLine(), is("some/string"));
    }
}
