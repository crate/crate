package crate.elasticsearch.module.restore.test;

import com.github.tlrx.elasticsearch.test.EsSetup;
import crate.elasticsearch.action.dump.DumpAction;
import crate.elasticsearch.action.export.ExportAction;
import crate.elasticsearch.action.export.ExportRequest;
import crate.elasticsearch.action.export.ExportResponse;
import crate.elasticsearch.action.import_.ImportRequest;
import crate.elasticsearch.action.import_.ImportResponse;
import crate.elasticsearch.action.restore.RestoreAction;
import crate.elasticsearch.module.AbstractRestActionTest;
import junit.framework.TestCase;

import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.github.tlrx.elasticsearch.test.EsSetup.*;

public class RestRestoreActionTest extends AbstractRestActionTest {

    /**
     * Restore previously dumped data from the default location
     */
    @Test
    public void testRestoreDumpedData() {

        deleteDefaultDir();

        setUpSecondNode();
        // create sample data
        esSetup.execute(deleteAll(), createIndex("users").withSettings(
                fromClassPath("essetup/settings/test_a.json")).withMapping("d",
                        fromClassPath("essetup/mappings/test_a.json")));
        esSetup.execute(index("users", "d", "1").withSource("{\"name\": \"item1\"}"));
        esSetup.execute(index("users", "d", "2").withSource("{\"name\": \"item2\"}"));
        esSetup2.client().admin().cluster().prepareHealth().setWaitForGreenStatus().
            setWaitForNodes("2").setWaitForRelocatingShards(0).execute().actionGet();

        // dump data and recreate empty index
        executeDumpRequest("");

        // delete all
        esSetup.execute(deleteAll());
        esSetup.client().admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        // run restore without pyload relative directory
        ImportResponse response = executeRestoreRequest("");
        List<Map<String, Object>> imports = getImports(response);
        assertEquals(2, imports.size());

        assertTrue(existsWithField("1", "name", "item1", "users", "d"));
        assertTrue(existsWithField("2", "name", "item2", "users", "d"));

        ClusterStateRequest clusterStateRequest = Requests.clusterStateRequest().filteredIndices("users");
        IndexMetaData metaData = esSetup.client().admin().cluster().state(clusterStateRequest).actionGet().getState().metaData().index("users");
        assertEquals("{\"d\":{\"properties\":{\"name\":{\"type\":\"string\",\"index\":\"not_analyzed\",\"store\":true,\"omit_norms\":true,\"index_options\":\"docs\"}}}}",
                metaData.mappings().get("d").source().toString());
        assertEquals(2, metaData.numberOfShards());
        assertEquals(0, metaData.numberOfReplicas());
    }


    private boolean existsWithField(String id, String field, String value, String index, String type) {
        GetRequestBuilder rb = new GetRequestBuilder(esSetup.client(), index);
        GetResponse res = rb.setType(type).setId(id).execute().actionGet();
        return res.isExists() && res.getSourceAsMap().get(field).equals(value);
    }

    private static List<Map<String, Object>> getImports(ImportResponse resp) {
        return get(resp, "imports");
    }

    private static List<Map<String, Object>> get(ImportResponse resp, String key) {
        Map<String, Object> res = null;
        try {
            res = toMap(resp);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        return (List<Map<String, Object>>) res.get(key);
    }

    private ImportResponse executeRestoreRequest(String source) {
        ImportRequest request = new ImportRequest();
        request.source(source);
        return esSetup.client().execute(RestoreAction.INSTANCE, request).actionGet();
    }

    private ExportResponse executeDumpRequest(String source) {
        ExportRequest exportRequest = new ExportRequest();
        exportRequest.source(source);
        return esSetup.client().execute(DumpAction.INSTANCE, exportRequest).actionGet();
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

}
