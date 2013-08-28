package crate.elasticsearch.module.export.test;

import static com.github.tlrx.elasticsearch.test.EsSetup.createIndex;
import static com.github.tlrx.elasticsearch.test.EsSetup.deleteAll;
import static com.github.tlrx.elasticsearch.test.EsSetup.fromClassPath;
import static com.github.tlrx.elasticsearch.test.EsSetup.index;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.elasticsearch.client.Client;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.indices.IndexMissingException;
import org.junit.Test;

import com.github.tlrx.elasticsearch.test.EsSetup;

import crate.elasticsearch.action.export.ExportAction;
import crate.elasticsearch.action.export.ExportRequest;
import crate.elasticsearch.action.export.ExportResponse;
import crate.elasticsearch.module.AbstractRestActionTest;

public class RestExportActionTest extends AbstractRestActionTest {

    /**
     * Either one of the parameters 'output_cmd' or 'output_file' is required.
     */
    @Test
    public void testNoCommandOrFile() throws IOException {
        ExportResponse response = executeExportRequest("{\"fields\": [\"name\"]}");
        assertEquals(2, response.getShardFailures().length);
        assertTrue(response.getShardFailures()[0].reason().contains(
                "'output_cmd' or 'output_file' has not been defined"));
        assertTrue(response.getShardFailures()[1].reason().contains(
                "'output_cmd' or 'output_file' has not been defined"));
    }

    /**
     * The parameter 'fields' is required.
     */
    @Test
    public void testNoExportFields() {
        ExportResponse response = executeExportRequest("{\"output_cmd\": \"cat\"}");

        List<Map<String, Object>> infos = getExports(response);
        assertEquals(0, infos.size());
        assertEquals(2, response.getShardFailures().length);
        assertTrue(response.getShardFailures()[0].reason().contains(
                "No export fields defined"));
        assertTrue(response.getShardFailures()[1].reason().contains(
                "No export fields defined"));
    }

    /**
     * Invalid parameters lead to an error response.
     */
    @Test
    public void testBadParserArgument() {
        ExportResponse response = executeExportRequest(
                "{\"output_cmd\": \"cat\", \"fields\": [\"name\"], \"badparam\":\"somevalue\"}");

        List<Map<String, Object>> infos = getExports(response);
        assertEquals(0, infos.size());
        assertEquals(2, response.getShardFailures().length);
        assertTrue(response.getShardFailures()[0].reason().contains(
                "No parser for element [badparam]"));
        assertTrue(response.getShardFailures()[1].reason().contains(
                "No parser for element [badparam]"));
    }

    /**
     * The 'output_cmd' parameter can be a single command and is executed. The
     * response shows the index, the node name, the shard number, the executed
     * command, the exit code of the process and the process' standard out and
     * standard error logs (first 8K) of every shard result.
     */
    @Test
    public void testSingleOutputCommand() {
        ExportResponse response = executeExportRequest(
                "{\"output_cmd\": \"cat\", \"fields\": [\"name\"]}");

        List<Map<String, Object>> infos = getExports(response);
        assertEquals(2, infos.size());
        assertShardInfoCommand(infos.get(0), "users", 0,
                "{\"name\":\"car\"}\n{\"name\":\"train\"}\n", "", null);
        assertShardInfoCommand(infos.get(1), "users", 0,
                "{\"name\":\"bike\"}\n{\"name\":\"bus\"}\n", "", null);
    }

    /**
     * The 'output_cmd' parameter can also be a list of arguments.
     */
    @Test
    public void testOutputCommandList() {
        ExportResponse response = executeExportRequest(
                "{\"output_cmd\": [\"/bin/sh\", \"-c\", \"cat\"], \"fields\": [\"name\"]}");

        List<Map<String, Object>> infos = getExports(response);
        assertEquals(2, infos.size());
        assertShardInfoCommand(infos.get(0), "users", 0,
                "{\"name\":\"car\"}\n{\"name\":\"train\"}\n", "", null);
        assertShardInfoCommand(infos.get(1), "users", 0,
                "{\"name\":\"bike\"}\n{\"name\":\"bus\"}\n", "", null);
    }

    /**
     * The gzip compression will also work on output commands.
     */
    @Test
    public void testOutputCommandWithGZIP() {
        ExportResponse response = executeExportRequest(
                "{\"output_cmd\": [\"/bin/sh\", \"-c\", \"gunzip\"], \"fields\": [\"name\"], \"compression\": \"gzip\"}");

        List<Map<String, Object>> infos = getExports(response);
        assertEquals(2, infos.size());
        assertShardInfoCommand(infos.get(0), "users", 0,
                "{\"name\":\"car\"}\n{\"name\":\"train\"}\n", "", null);
        assertShardInfoCommand(infos.get(1), "users", 0,
                "{\"name\":\"bike\"}\n{\"name\":\"bus\"}\n", "", null);
    }

    /**
     * The 'output_file' parameter defines the filename to save the export.
     * There are 3 template variables that will be replaced:
     * <p/>
     * - ${cluster} : will be replaced with the cluster name - ${index} : will
     * be replaced with the index name - ${shard} : will be replaced with the
     * shard name
     * <p/>
     * The response contains the index, the shard number, the node name and the
     * generated output file name of every shard result.
     */
    @Test
    public void testOutputFile() {
        String clusterName = esSetup.client().admin().cluster().prepareHealth().
                setWaitForGreenStatus().execute().actionGet().getClusterName();
        String filename_0 = "/tmp/" + clusterName + ".0.users.export";
        String filename_1 = "/tmp/" + clusterName + ".1.users.export";
        new File(filename_0).delete();
        new File(filename_1).delete();

        ExportResponse response = executeExportRequest(
                "{\"output_file\": \"/tmp/${cluster}.${shard}.${index}.export\", \"fields\": [\"name\", \"_id\"]}");

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

        List<String> lines_0 = readLines(filename_0);
        assertEquals(2, lines_0.size());
        assertEquals("{\"name\":\"car\",\"_id\":\"1\"}", lines_0.get(0));
        assertEquals("{\"name\":\"train\",\"_id\":\"3\"}", lines_0.get(1));
        List<String> lines_1 = readLines(filename_1);
        assertEquals(2, lines_1.size());
        assertEquals("{\"name\":\"bike\",\"_id\":\"2\"}", lines_1.get(0));
        assertEquals("{\"name\":\"bus\",\"_id\":\"4\"}", lines_1.get(1));
    }

    public void testGZIPOutputFile() {
        String clusterName = esSetup.client().admin().cluster().prepareHealth().
                setWaitForGreenStatus().execute().actionGet().getClusterName();
        String filename_0 = "/tmp/" + clusterName + ".0.users.zipexport.gz";
        String filename_1 = "/tmp/" + clusterName + ".1.users.zipexport.gz";
        new File(filename_0).delete();
        new File(filename_1).delete();

        ExportResponse response = executeExportRequest(
                "{\"output_file\": \"/tmp/${cluster}.${shard}.${index}.zipexport.gz\", \"fields\": [\"name\", \"_id\"], \"compression\": \"gzip\"}");

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
        assertEquals("{\"name\":\"car\",\"_id\":\"1\"}", lines_0.get(0));
        assertEquals("{\"name\":\"train\",\"_id\":\"3\"}", lines_0.get(1));
        List<String> lines_1 = readLinesFromGZIP(filename_1);
        assertEquals(2, lines_1.size());
        assertEquals("{\"name\":\"bike\",\"_id\":\"2\"}", lines_1.get(0));
        assertEquals("{\"name\":\"bus\",\"_id\":\"4\"}", lines_1.get(1));
    }

    /**
     * Only one parameter of the two 'output_file' or 'output_cmd' can be used.
     */
    @Test
    public void testOutputFileAndOutputCommand() {
        ExportResponse response = executeExportRequest(
                "{\"output_file\": \"/filename\", \"output_cmd\": \"cat\", \"fields\": [\"name\"]}");

        List<Map<String, Object>> infos = getExports(response);
        assertEquals(0, infos.size());
        assertEquals(0, infos.size());
        assertEquals(2, response.getShardFailures().length);
        assertTrue(response.getShardFailures()[0].reason().contains(
                "Concurrent definition of 'output_cmd' and 'output_file'"));
        assertTrue(response.getShardFailures()[1].reason().contains(
                "Concurrent definition of 'output_cmd' and 'output_file'"));

    }

    /**
     * The 'force_overwrite' parameter forces existing files to be overwritten.
     */
    @Test
    public void testForceOverwrite() {
        String filename = "/tmp/filename.export";
        ExportResponse response = executeExportRequest("{\"output_file\": \"" + filename +
                "\", \"fields\": [\"name\"], \"force_overwrite\": \"true\"}");

        List<Map<String, Object>> infos = getExports(response);
        assertEquals(2, infos.size());
        assertEquals("/tmp/filename.export", infos.get(0).get("output_file").toString());
        assertEquals("/tmp/filename.export", infos.get(1).get("output_file").toString());
        List<String> lines = readLines(filename);
        assertEquals(2, lines.size());
        assertEquals("{\"name\":\"bike\"}", lines.get(0));
    }

    /**
     * The explain parameter does a dry-run without running the command. The
     * response therefore does not contain the stderr, stdout and exitcode
     * values.
     */
    @Test
    public void testExplainCommand() {
        ExportResponse response = executeExportRequest(
                "{\"output_cmd\": \"cat\", \"fields\": [\"name\"], \"explain\": \"true\"}");

        List<Map<String, Object>> infos = getExports(response);
        assertEquals(2, infos.size());
        Map<String, Object> shard_info = infos.get(0);
        assertFalse(shard_info.containsKey("stderr"));
        assertFalse(shard_info.containsKey("stdout"));
        assertFalse(shard_info.containsKey("exitcode"));
        assertSame(shard_info.keySet(), infos.get(0).keySet());
    }

    /**
     * The explain parameter does a dry-run without writing to the file.
     */
    @Test
    public void testExplainFile() {
        String filename = "/tmp/explain.txt";
        new File(filename).delete();

        executeExportRequest("{\"output_file\": \"" + filename +
                "\", \"fields\": [\"name\"], \"explain\": \"true\"}");

        assertFalse(new File(filename).exists());
    }

    /**
     * Export request must also work with multiple nodes.
     */
    @Test
    public void testWithMultipleNodes() {
        // Prepare a second node and wait for relocation
        esSetup2 = new EsSetup();
        esSetup2.execute(index("users", "d").withSource("{\"name\": \"motorbike\"}"));
        esSetup2.client().admin().cluster().prepareHealth().setWaitForGreenStatus().
            setWaitForNodes("2").setWaitForRelocatingShards(0).execute().actionGet();

        // Do export request
        String source = "{\"output_cmd\": \"cat\", \"fields\": [\"name\"]}";
        ExportRequest exportRequest = new ExportRequest();
        exportRequest.source(source);
        ExportResponse response = esSetup2.client().execute(
                ExportAction.INSTANCE, exportRequest).actionGet();

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
        ExportResponse response = executeExportRequest(
                "{\"output_file\": \"/tmp/query-${shard}.json\", \"fields\": [\"name\"], " +
                "\"query\": {\"match\": {\"name\":\"bus\"}}, \"force_overwrite\": true}");

        assertEquals(0, response.getFailedShards());
        List<Map<String, Object>> infos = getExports(response);
        assertEquals(2, infos.size());

        List<String> lines_0 = readLines("/tmp/query-0.json");
        assertEquals(0, lines_0.size());
        List<String> lines_1 = readLines("/tmp/query-1.json");
        assertEquals(1, lines_1.size());
        assertEquals("{\"name\":\"bus\"}", lines_1.get(0));
    }

    /**
     * Only the compression format 'gzip' or no compression is supported.
     */
    @Test
    public void testUnsopportedCompressionFormat() {
        String clusterName = esSetup.client().admin().cluster().prepareHealth().
                setWaitForGreenStatus().execute().actionGet().getClusterName();
        String filename_0 = "/tmp/" + clusterName + ".0.users.nocompressexport.gz";
        String filename_1 = "/tmp/" + clusterName + ".1.users.nocompressexport.gz";
        new File(filename_0).delete();
        new File(filename_1).delete();
        ExportResponse response = executeExportRequest(
                "{\"output_file\": \"/tmp/${cluster}.${shard}.${index}.lzivexport.gz\", \"fields\": [\"name\", \"_id\"], \"compression\": \"LZIV\"}");

        assertEquals(2, response.getFailedShards());
        assertTrue(response.getShardFailures()[0].reason().contains(
                "Compression format 'lziv' unknown or not supported."));

        ExportResponse response2 = executeExportRequest(
                "{\"output_file\": \"/tmp/${cluster}.${shard}.${index}.nocompressexport.gz\", \"fields\": [\"name\", \"_id\"], \"compression\": \"\"}");

        assertEquals(0, response2.getFailedShards());
        assertEquals(2, response2.getSuccessfulShards());
    }

    /**
     * The field _version returns the correct version.
     */
    @Test
    public void testVersion() {
        esSetup.execute(index("users", "d", "2").withSource("{\"name\": \"electric bike\"}"));
        ExportResponse response = executeExportRequest(
                "{\"output_cmd\": \"cat\", \"fields\": [\"_id\", \"_version\", \"_source\"]}");

        List<Map<String, Object>> infos = getExports(response);
        assertEquals(2, infos.size());
        assertShardInfoCommand(infos.get(1), "users", 0,
                "{\"_id\":\"4\",\"_version\":1,\"_source\":{\"name\":\"bus\"}}\n{\"_id\":\"2\",\"_version\":2,\"_source\":{\"name\":\"electric bike\"}}\n",
                "", null);

    }

    /**
     * External versions can start with 0 and also are able to get exported.
     */
    @Test
    public void testExternalVersion() {
        Client client = esSetup.client();
        deleteIndex("test");
        client.admin().indices().prepareCreate("test").execute().actionGet();
        client.admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet();

        client.prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(
                0).setVersionType(VersionType.EXTERNAL).execute().actionGet();
        client.admin().indices().prepareRefresh().execute().actionGet();

        ExportResponse response = executeExportRequest(
                "{\"output_cmd\": \"cat\", \"fields\": [\"_id\", \"_version\", \"_source\"]}");

        List<Map<String, Object>> infos = getExports(response);
        assertEquals(3, infos.size());
        assertEquals(
                "{\"_id\":\"1\",\"_version\":0,\"_source\":{\"field1\":\"value1_1\"}}\n",
                infos.get(0).get("stdout"));
        deleteIndex("test");
    }

    /**
     * The _timestamp field is not returned if the mapping does not store the timestamps.
     */
    @Test
    public void testTimestampNotStored() {
        ExportResponse response = executeExportRequest(
                "{\"output_cmd\": \"cat\", \"fields\": [\"_id\", \"_timestamp\"]}");
        List<Map<String, Object>> infos = getExports(response);
        assertEquals("{\"_id\":\"1\"}\n{\"_id\":\"3\"}\n", infos.get(0).get("stdout"));
    }

    /**
     * Create a mapping with "_timestamp": {"enabled": true, "store": "yes"} to get it
     * as a field.
     */
    @Test
    public void testTimestampStored(){
        esSetup.execute(deleteAll(), createIndex("tsstored").withSettings(
                fromClassPath("essetup/settings/test_a.json")).withMapping("d",
                        "{\"d\": {\"_timestamp\": {\"enabled\": true, \"store\": \"yes\"}}}"));
        Client client = esSetup.client();
        client.prepareIndex("tsstored", "d", "1").setSource(
                "field1", "value1").setTimestamp("123").execute().actionGet();
        client.admin().indices().prepareRefresh().execute().actionGet();

        ExportResponse response = executeExportRequest(
                "{\"output_cmd\": \"cat\", \"fields\": [\"_id\", \"_timestamp\"]}");

        List<Map<String, Object>> infos = getExports(response);
        assertEquals("{\"_id\":\"1\",\"_timestamp\":123}\n", infos.get(1).get("stdout"));
    }

    /**
     * If _ttl is not enabled in the mapping, the _ttl field is not in the output.
     */
    public void testTTLNotEnabled() {
        ExportResponse response = executeExportRequest(
                "{\"output_cmd\": \"cat\", \"fields\": [\"_id\", \"_ttl\"]}");
        List<Map<String, Object>> infos = getExports(response);
        assertEquals("{\"_id\":\"1\"}\n{\"_id\":\"3\"}\n", infos.get(0).get("stdout"));
    }

    /**
     * If _ttl is enabled in the mapping, the _ttl field delivers the time stamp
     * when the object is expired.
     */
    @Test
    public void testTTLEnabled() {
        esSetup.execute(deleteAll(), createIndex("ttlenabled").withSettings(
                fromClassPath("essetup/settings/test_a.json")).withMapping("d",
                        "{\"d\": {\"_ttl\": {\"enabled\": true, \"default\": \"1d\"}}}"));
        Client client = esSetup.client();
        client.prepareIndex("ttlenabled", "d", "1").setSource("field1", "value1").execute().actionGet();
        client.admin().indices().prepareRefresh().execute().actionGet();

        Date now = new Date();
        ExportResponse response = executeExportRequest(
                "{\"output_cmd\": \"cat\", \"fields\": [\"_id\", \"_ttl\"]}");
        List<Map<String, Object>> infos = getExports(response);
        String stdout = infos.get(1).get("stdout").toString();
        assertTrue(stdout.startsWith("{\"_id\":\"1\",\"_ttl\":"));
        String lsplit  = stdout.substring(18);
        long ttl = Long.valueOf(lsplit.substring(0, lsplit.length() - 2));
        long diff = ttl - now.getTime();
        assertTrue(diff < 86400000 && diff > 86390000);
    }


    /**
     * The _index and _type fields can be fetched for every object.
     */
    @Test
    public void testIndexAndType() {
        ExportResponse response = executeExportRequest(
                "{\"output_cmd\": \"cat\", \"fields\": [\"_id\", \"_type\", \"_index\"]}");
        List<Map<String, Object>> infos = getExports(response);
        assertEquals("{\"_id\":\"2\",\"_type\":\"d\",\"_index\":\"users\"}\n" +
                "{\"_id\":\"4\",\"_type\":\"d\",\"_index\":\"users\"}\n",
                infos.get(1).get("stdout"));
    }

    /**
     * The _routing field delivers the routing value if one is given.
     */
    @Test
    public void testRouting() {
        Client client = esSetup.client();
        client.prepareIndex("users", "d", "1").setSource("field1", "value1").setRouting("2").execute().actionGet();
        client.admin().indices().prepareRefresh().execute().actionGet();

        ExportResponse response = executeExportRequest(
                "{\"output_cmd\": \"cat\", \"fields\": [\"_id\", \"_source\", \"_routing\"]}");
        List<Map<String, Object>> infos = getExports(response);
        assertEquals("{\"_id\":\"2\",\"_source\":{\"name\":\"bike\"}}\n" +
                "{\"_id\":\"4\",\"_source\":{\"name\":\"bus\"}}\n" +
                "{\"_id\":\"1\",\"_source\":{\"field1\":\"value1\"},\"_routing\":\"2\"}\n",
                infos.get(1).get("stdout"));
    }

    /**
     * If the path of the output file is relative, the files are put to the data directory
     * of each node in a sub directory /export .
     */
    @Test
    public void testExportRelativeFilename() {
        esSetup2 = new EsSetup();
        esSetup2.execute(index("users", "d").withSource("{\"name\": \"motorbike\"}"));
        esSetup2.client().admin().cluster().prepareHealth().setWaitForGreenStatus().
            setWaitForNodes("2").setWaitForRelocatingShards(0).execute().actionGet();

        ExportResponse response = executeExportRequest(
                "{\"output_file\": \"export.${shard}.${index}.json\", \"fields\": [\"name\", \"_id\"], \"force_overwrite\": true}");

        List<Map<String, Object>> infos = getExports(response);
        assertEquals(2, infos.size());
        String output_file_0 = infos.get(0).get("output_file").toString();
        String output_file_1 = infos.get(1).get("output_file").toString();
        Pattern p = Pattern.compile("(.*)/nodes/(\\d)/export.(\\d).users.json");
        Matcher m0 = p.matcher(output_file_0);
        Matcher m1 = p.matcher(output_file_1);
        assertTrue(m0.find());
        assertTrue(m1.find());
        assertTrue(m0.group(2) != m1.group(2));
    }

    /**
     * If the target folder does not allow to write to the export will abort with a proper response
     */
    @Test
    public void testPermissions() {
        File restrictedFolder = new File("/tmp/testRestricted");
        if (restrictedFolder.exists()) {
            for (File c : restrictedFolder.listFiles()) {
                c.delete();
            }
            restrictedFolder.delete();
        }
        restrictedFolder.mkdir();
        restrictedFolder.setWritable(false);

        ExportResponse response = executeExportRequest(
                "{\"output_file\": \"/tmp/testRestricted/export.json\", \"fields\": [\"_id\"]}");
        assertEquals(2, response.getFailedShards());
        assertTrue(response.getShardFailures()[0].reason().contains(
                "Insufficient permissions to write into /tmp/testRestricted"));
        restrictedFolder.delete();
    }

    @Test
    public void testSettings() throws IOException {
        String clusterName = esSetup.client().admin().cluster().prepareHealth().
                setWaitForGreenStatus().execute().actionGet().getClusterName();
        String filename_0 = "/tmp/" + clusterName + ".0.users.export";
        String filename_1 = "/tmp/" + clusterName + ".1.users.export";
        new File(filename_0).delete();
        new File(filename_1).delete();
        new File(filename_0 += ".settings").delete();
        new File(filename_1 += ".settings").delete();

        ExportResponse response = executeExportRequest(
                "{\"output_file\": \"/tmp/${cluster}.${shard}.${index}.export\", \"fields\": [\"name\", \"_id\"], \"settings\": true}");

        List<Map<String, Object>> infos = getExports(response);
        assertEquals(2, infos.size());
        String settings_0 = new BufferedReader(new FileReader(new File(filename_0))).readLine();
        assertTrue(settings_0.matches("\\{\"users\":\\{\"settings\":\\{\"index.number_of_replicas\":\"0\",\"index.number_of_shards\":\"2\",\"index.version.created\":\"(.*)\"\\}\\}\\}"));
        String settings_1 = new BufferedReader(new FileReader(new File(filename_1))).readLine();
        assertTrue(settings_1.matches("\\{\"users\":\\{\"settings\":\\{\"index.number_of_replicas\":\"0\",\"index.number_of_shards\":\"2\",\"index.version.created\":\"(.*)\"\\}\\}\\}"));
    }

    @Test
    public void testSettingsFileExists() throws IOException {
        testSettings();
        ExportResponse response = executeExportRequest(
                "{\"output_file\": \"/tmp/${cluster}.${shard}.${index}.export\", \"fields\": [\"name\", \"_id\"], \"settings\": true}");
        List<Map<String, Object>> infos = getExports(response);
        assertEquals(0, infos.size());
        assertEquals(2, response.getShardFailures().length);
        assertTrue(response.getShardFailures()[0].reason().contains(
                "Export Failed [Failed to write settings for index users]]; nested: IOException[File exists"));
    }

    @Test
    public void testSettingsWithOutputCmd() {
        ExportResponse response = executeExportRequest(
                "{\"output_cmd\": \"cat\", \"fields\": [\"name\", \"_id\"], \"settings\": true}");
        List<Map<String, Object>> infos = getExports(response);
        assertEquals(0, infos.size());
        assertTrue(response.getShardFailures()[0].reason().contains("Parse Failure [Parameter 'settings' requires usage of 'output_file']]"));
    }

    public void testMappings() throws IOException {
        String clusterName = esSetup.client().admin().cluster().prepareHealth().
                setWaitForGreenStatus().execute().actionGet().getClusterName();
        String filename_0 = "/tmp/" + clusterName + ".0.users.export";
        String filename_1 = "/tmp/" + clusterName + ".1.users.export";
        new File(filename_0).delete();
        new File(filename_1).delete();
        new File(filename_0 += ".mapping").delete();
        new File(filename_1 += ".mapping").delete();

        ExportResponse response = executeExportRequest(
                "{\"output_file\": \"/tmp/${cluster}.${shard}.${index}.export\", \"fields\": [\"name\", \"_id\"], \"mappings\": true}");

        List<Map<String, Object>> infos = getExports(response);
        assertEquals(2, infos.size());
        String mappings_0 = new BufferedReader(new FileReader(new File(filename_0))).readLine();
        assertEquals("{\"users\":{\"d\":{\"properties\":{\"name\":{\"type\":\"string\",\"index\":\"not_analyzed\",\"store\":true,\"omit_norms\":true,\"index_options\":\"docs\"}}}}}", mappings_0);
        String mappings_1 = new BufferedReader(new FileReader(new File(filename_1))).readLine();
        assertEquals("{\"users\":{\"d\":{\"properties\":{\"name\":{\"type\":\"string\",\"index\":\"not_analyzed\",\"store\":true,\"omit_norms\":true,\"index_options\":\"docs\"}}}}}", mappings_1);
    }

    @Test
    public void testMappingsFileExists() throws IOException {
        testMappings();
        ExportResponse response = executeExportRequest(
                "{\"output_file\": \"/tmp/${cluster}.${shard}.${index}.export\", \"fields\": [\"name\", \"_id\"], \"mappings\": true}");
        List<Map<String, Object>> infos = getExports(response);
        assertEquals(0, infos.size());
        assertEquals(2, response.getShardFailures().length);
        assertTrue(response.getShardFailures()[0].reason().contains(
                "Export Failed [Failed to write mappings for index users]]; nested: IOException[File exists"));
    }

    @Test
    public void testMappingsWithOutputCmd() {
        ExportResponse response = executeExportRequest(
                "{\"output_cmd\": \"cat\", \"fields\": [\"name\", \"_id\"], \"mappings\": true}");
        List<Map<String, Object>> infos = getExports(response);
        assertEquals(0, infos.size());
        assertTrue(response.getShardFailures()[0].reason().contains("Parse Failure [Parameter 'mappings' requires usage of 'output_file']]"));
    }


    private boolean deleteIndex(String name) {
        try {
            esSetup.client().admin().indices().prepareDelete(name).execute().actionGet();
        } catch (IndexMissingException e) {
            return false;
        }
        return true;
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
     * Execute an export request with a JSON string as source query. Waits for
     * async callback and writes result in response member variable.
     *
     * @param source
     */
    private ExportResponse executeExportRequest(String source) {
        ExportRequest exportRequest = new ExportRequest();
        exportRequest.source(source);
        return esSetup.client().execute(ExportAction.INSTANCE, exportRequest).actionGet();
    }

    /**
     * Get a list of lines from a file.
     * Test fails if file not found or IO exception happens.
     *
     * @param filename the file name to read
     * @return a list of strings
     */
    private List<String> readLines(String filename) {
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(new File(filename)));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            fail("File not found");
        }
        return readLines(filename, reader);
    }

    private void assertShardInfoCommand(Map<String, Object> map, String index,
            int exitcode, String stdout, String stderr, String cmd) {
        assertEquals(index, map.get("index"));
        assertEquals(exitcode, map.get("exitcode"));
        assertEquals(stderr, map.get("stderr"));
        assertEquals(stdout, map.get("stdout"));
        assertEquals(cmd, map.get("cmd"));
        assertTrue(map.containsKey("node_id"));
    }
}
