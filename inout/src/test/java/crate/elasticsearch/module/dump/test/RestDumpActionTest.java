package crate.elasticsearch.module.dump.test;

import static com.github.tlrx.elasticsearch.test.EsSetup.index;

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

import org.junit.Test;

import com.github.tlrx.elasticsearch.test.EsSetup;

import crate.elasticsearch.action.dump.DumpAction;
import crate.elasticsearch.action.export.ExportAction;
import crate.elasticsearch.action.export.ExportRequest;
import crate.elasticsearch.action.export.ExportResponse;
import crate.elasticsearch.module.AbstractRestActionTest;

public class RestDumpActionTest extends AbstractRestActionTest {

    /**
     * Without any given payload the dump endpoint will export to the default location
     * ``dump`` within the data folder of each node
     */
    public void testNoOption() throws IOException {
        deleteDefaultDir();
        ExportResponse response = executeDumpRequest();

        List<Map<String, Object>> infos = getExports(response);
        assertEquals(2, infos.size());
        Map<String, Object> shard_0 = infos.get(0);
        Map<String, Object> shard_1 = infos.get(1);
        assertEquals("users", shard_0.get("index"));
        assertEquals("users", shard_1.get("index"));
        String output_file_0 = shard_0.get("output_file").toString();
        String output_file_1 = shard_1.get("output_file").toString();
        assertTrue(shard_0.containsKey("node_id"));
        assertTrue(shard_1.containsKey("node_id"));

        List<String> lines_0 = readLinesFromGZIP(output_file_0);
        assertEquals(2, lines_0.size());
        assertEquals("{\"_id\":\"1\",\"_source\":{\"name\":\"car\"},\"_version\":1,\"_index\":\"users\",\"_type\":\"d\"}", lines_0.get(0));
        assertEquals("{\"_id\":\"3\",\"_source\":{\"name\":\"train\"},\"_version\":1,\"_index\":\"users\",\"_type\":\"d\"}", lines_0.get(1));
        List<String> lines_1 = readLinesFromGZIP(output_file_1);
        assertEquals(2, lines_1.size());
        assertEquals("{\"_id\":\"2\",\"_source\":{\"name\":\"bike\"},\"_version\":1,\"_index\":\"users\",\"_type\":\"d\"}", lines_1.get(0));
        assertEquals("{\"_id\":\"4\",\"_source\":{\"name\":\"bus\"},\"_version\":1,\"_index\":\"users\",\"_type\":\"d\"}", lines_1.get(1));
    }


    /**
     * Invalid parameters lead to an error response.
     */
    @Test
    public void testBadParserArgument() {
        ExportResponse response = executeDumpRequest(
                "{\"badparam\":\"somevalue\"}");

        List<Map<String, Object>> infos = getExports(response);
        assertEquals(0, infos.size());
        assertEquals(2, response.getShardFailures().length);
        assertTrue(response.getShardFailures()[0].reason().contains(
                "No parser for element [badparam]"));
        assertTrue(response.getShardFailures()[1].reason().contains(
                "No parser for element [badparam]"));
    }

    /**
     * The target directory must exist
     */
    @Test
    public void testDirMustExist() {
        ExportResponse response = executeDumpRequest(
                "{\"directory\": \"/tmp/doesnotexist\"}");
        List<Map<String, Object>> infos = getExports(response);
        assertEquals(0, infos.size());
        assertEquals(2, response.getShardFailures().length);
        assertTrue(response.getShardFailures()[0].reason().contains(
                "Target folder /tmp/doesnotexist does not exist"));
        assertTrue(response.getShardFailures()[1].reason().contains(
                "Target folder /tmp/doesnotexist does not exist"));
    }

    /**
     * The 'directory' parameter defines the directory to save the export.
     * Each exported file will be called ${cluster}_${index}_${shard}.json.gz
     * These 3 template variables will be replaced:
     * <p/>
     * - ${cluster} : will be replaced with the cluster name - ${index} : will
     * be replaced with the index name - ${shard} : will be replaced with the
     * shard name
     * <p/>
     * Also the .settings and .mapping files will be generated in the same path.
     * <p/>
     * The response contains the index, the shard number, the node name and the
     * generated output file name of every shard result.
     */
    @Test
    public void testDirectory() {
        String clusterName = esSetup.client().admin().cluster().prepareHealth().
                setWaitForGreenStatus().execute().actionGet().getClusterName();
        String filename_0 = "/tmp/" + clusterName + "_users_0.json.gz";
        String filename_1 = "/tmp/" + clusterName + "_users_1.json.gz";
        new File(filename_0).delete();
        new File(filename_1).delete();
        new File(filename_0 + ".settings").delete();
        new File(filename_1 + ".settings").delete();
        new File(filename_0 + ".mapping").delete();
        new File(filename_1 + ".mapping").delete();

        ExportResponse response = executeDumpRequest(
                "{\"directory\": \"/tmp\"}");

        List<Map<String, Object>> infos = getExports(response);
        assertEquals(2, infos.size());
        Map<String, Object> shard_0 = infos.get(0);
        Map<String, Object> shard_1 = infos.get(1);
        assertEquals("users", shard_0.get("index"));
        assertEquals("users", shard_1.get("index"));
        String output_file_0 = shard_0.get("output_file").toString();
        assertEquals(filename_0, output_file_0);
        String output_file_1 = shard_1.get("output_file").toString();
        assertEquals(filename_1, output_file_1);
        assertTrue(shard_0.containsKey("node_id"));
        assertTrue(shard_1.containsKey("node_id"));

        List<String> lines_0 = readLinesFromGZIP(filename_0);
        assertEquals(2, lines_0.size());
        assertEquals("{\"_id\":\"1\",\"_source\":{\"name\":\"car\"},\"_version\":1,\"_index\":\"users\",\"_type\":\"d\"}", lines_0.get(0));
        assertEquals("{\"_id\":\"3\",\"_source\":{\"name\":\"train\"},\"_version\":1,\"_index\":\"users\",\"_type\":\"d\"}", lines_0.get(1));
        List<String> lines_1 = readLinesFromGZIP(filename_1);
        assertEquals(2, lines_1.size());
        assertEquals("{\"_id\":\"2\",\"_source\":{\"name\":\"bike\"},\"_version\":1,\"_index\":\"users\",\"_type\":\"d\"}", lines_1.get(0));
        assertEquals("{\"_id\":\"4\",\"_source\":{\"name\":\"bus\"},\"_version\":1,\"_index\":\"users\",\"_type\":\"d\"}", lines_1.get(1));

        assertTrue(new File(filename_0 + ".settings").exists());
        assertTrue(new File(filename_0 + ".mapping").exists());
        assertTrue(new File(filename_1 + ".settings").exists());
        assertTrue(new File(filename_1 + ".mapping").exists());
    }


    /**
     * The 'force_overwrite' parameter forces existing files to be overwritten.
     */
    @Test
    public void testForceOverwrite() {

        // make sure target directory exists and is empty
        File dumpDir = new File("/tmp/forceDump");
        if (dumpDir.exists()) {
            for (File c : dumpDir.listFiles()) {
                c.delete();
            }
            dumpDir.delete();
        }
        dumpDir.mkdir();

        // initial dump to target directory
        ExportResponse response = executeDumpRequest(
                "{\"directory\": \"/tmp/forceDump\"}");

        List<Map<String, Object>> infos = getExports(response);

        assertEquals(2, infos.size());
        assertEquals(0, response.getShardFailures().length);

        // second attempt to dump will fail
        response = executeDumpRequest(
                "{\"directory\": \"/tmp/forceDump\"}");

        infos = getExports(response);
        assertEquals(0, infos.size());
        assertEquals(2, response.getShardFailures().length);

        // if force_overwrite == true a second dump will succeed
        response = executeDumpRequest(
                "{\"directory\": \"/tmp/forceDump\", \"force_overwrite\":true}");

        infos = getExports(response);
        assertEquals(2, infos.size());
        assertEquals(0, response.getShardFailures().length);
    }

    /**
     * Dump request must also work with multiple nodes.
     */
    @Test
    public void testWithMultipleNodes() {

        // make sure target directory exists and is empty
        File dumpDir = new File("/tmp/multipleNodes");
        if (dumpDir.exists()) {
            for (File c : dumpDir.listFiles()) {
                c.delete();
            }
            dumpDir.delete();
        }
        dumpDir.mkdir();

        // Prepare a second node and wait for relocation
        esSetup2 = new EsSetup();
        esSetup2.execute(index("users", "d").withSource("{\"name\": \"motorbike\"}"));
        esSetup2.client().admin().cluster().prepareHealth().setWaitForGreenStatus().
            setWaitForNodes("2").setWaitForRelocatingShards(0).execute().actionGet();

        // Do dump request
        String source = "{\"force_overwrite\": true, \"directory\":\"/tmp/multipleNodes\"}";
        ExportRequest exportRequest = new ExportRequest();
        exportRequest.source(source);
        ExportResponse response = esSetup.client().execute(
                DumpAction.INSTANCE, exportRequest).actionGet();

        // The two shard results are from different nodes and have no failures
        assertEquals(0, response.getFailedShards());
        List<Map<String, Object>> infos = getExports(response);
        assertNotSame(infos.get(0).get("node_id"), infos.get(1).get("node_id"));
    }

    /**
     * A query must also work and deliver only the queried results.
     */
    @Test
    public void testWithQuery() {

        // make sure target directory exists and is empty
        File dumpDir = new File("/tmp/query");
        if (dumpDir.exists()) {
            for (File c : dumpDir.listFiles()) {
                c.delete();
            }
            dumpDir.delete();
        }
        dumpDir.mkdir();

        String clusterName = esSetup.client().admin().cluster().prepareHealth().
                setWaitForGreenStatus().execute().actionGet().getClusterName();
        String filename_0 = "/tmp/query/" + clusterName + "_users_0.json.gz";
        String filename_1 = "/tmp/query/" + clusterName + "_users_1.json.gz";
        ExportResponse response = executeDumpRequest(
                "{\"directory\": \"/tmp/query\", \"query\": {\"match\": {\"name\":\"bus\"}}}");

        assertEquals(0, response.getFailedShards());
        List<Map<String, Object>> infos = getExports(response);
        assertEquals(2, infos.size());

        List<String> lines_0 = readLinesFromGZIP(filename_0);
        assertEquals(0, lines_0.size());
        List<String> lines_1 = readLinesFromGZIP(filename_1);
        assertEquals(1, lines_1.size());
        assertEquals("{\"_id\":\"4\",\"_source\":{\"name\":\"bus\"},\"_version\":1,\"_index\":\"users\",\"_type\":\"d\"}", lines_1.get(0));
    }

    /**
     * Helper method to delete an already existing dump directory
     */
    private void deleteDefaultDir() {
        ExportRequest exportRequest = new ExportRequest();
        exportRequest.source("{\"output_file\": \"dump\", \"fields\": [\"_source\", \"_id\", \"_index\", \"_type\"], \"force_overwrite\": true, \"explain\": true}");
        ExportResponse explain = esSetup.client().execute(ExportAction.INSTANCE, exportRequest).actionGet();

        try {
            Map<String, Object> res = toMap(explain);
            List<Map<String, String>> list = (ArrayList<Map<String, String>>) res.get("exports");
            for (Map<String, String> map : list) {
                File defaultDir = new File(map.get("output_file").toString());
                if (defaultDir.exists()) {
                    for (File c : defaultDir.listFiles()) {
                        c.delete();
                    }
                    defaultDir.delete();
                }
            }
        } catch (IOException e) {
        }
    }

    private static List<Map<String, Object>> getExports(ExportResponse resp) {
        Map<String, Object> res = null;
        try {
            res = toMap(resp);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        return (List<Map<String, Object>>) res.get("exports");
    }

    /**
     * Execute a dump request with a JSON string as source query. Waits for
     * async callback and writes result in response member variable.
     *
     * @param source
     */
    private ExportResponse executeDumpRequest(String source) {
        ExportRequest exportRequest = new ExportRequest();
        exportRequest.source(source);
        return esSetup.client().execute(DumpAction.INSTANCE, exportRequest).actionGet();
    }

    /**
     * Execute a dump request with a JSON string as source query. Waits for
     * async callback and writes result in response member variable.
     */
    private ExportResponse executeDumpRequest() {
        ExportRequest exportRequest = new ExportRequest();
        return esSetup.client().execute(DumpAction.INSTANCE, exportRequest).actionGet();
    }
}
