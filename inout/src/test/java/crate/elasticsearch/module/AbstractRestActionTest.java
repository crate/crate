package crate.elasticsearch.module;

import static com.github.tlrx.elasticsearch.test.EsSetup.createIndex;
import static com.github.tlrx.elasticsearch.test.EsSetup.deleteAll;
import static com.github.tlrx.elasticsearch.test.EsSetup.fromClassPath;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import junit.framework.TestCase;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.After;
import org.junit.Before;

import com.github.tlrx.elasticsearch.test.EsSetup;

/**
 * Abstract base class for the plugin's rest action tests. Sets up the client
 * and delivers some base functionality needed for all tests.
 */
public abstract class AbstractRestActionTest extends TestCase {

    protected EsSetup esSetup, esSetup2;

    @Before
    public void setUp() {
        esSetup = new EsSetup();
        esSetup.execute(deleteAll(), createIndex("users").withSettings(
                fromClassPath("essetup/settings/test_a.json")).withMapping("d",
                        fromClassPath("essetup/mappings/test_a.json")).withData(
                                fromClassPath("essetup/data/test_a.json")));
        esSetup.client().admin().indices().prepareRefresh("users").execute();
    }

    @After
    public void tearDown() {
        esSetup.terminate();
        if (esSetup2 != null) {
            esSetup2.terminate();
        }
    }

    /**
     * Convert an XContent object to a Java map
     * @param toXContent
     * @return
     * @throws IOException
     */
    public static Map<String, Object> toMap(ToXContent toXContent) throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        toXContent.toXContent(builder, ToXContent.EMPTY_PARAMS);
        return XContentFactory.xContent(XContentType.JSON).createParser(
                builder.string()).mapOrderedAndClose();
    }

    /**
     * Set up a second node and wait for green status
     */
    protected void setUpSecondNode() {
        esSetup2 = new EsSetup();
        esSetup2.execute(deleteAll());
        esSetup2.client().admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
    }

    /**
     * Get a list of lines from a gzipped file.
     * Test fails if file not found or IO exception happens.
     *
     * @param filename the file name to read
     * @return a list of strings
     */
    protected List<String> readLinesFromGZIP(String filename) {
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(
                    new GZIPInputStream(new FileInputStream(new File(filename)))));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            fail("File not found");
        } catch (IOException e) {
            e.printStackTrace();
            fail("IO Excsption while reading ZIP stream");
        }
        return readLines(filename, reader);
    }

    protected List<String> readLines(String filename, BufferedReader reader) {
        List<String> lines = new ArrayList<String>();
        try {
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
            fail("IO Exception occured while reading file");
        }
        return lines;
    }


}
