package org.cratedb;

import org.cratedb.action.sql.SQLAction;
import org.cratedb.action.sql.SQLRequest;
import org.cratedb.action.sql.SQLRequestBuilder;
import org.cratedb.action.sql.SQLResponse;
import org.cratedb.test.integration.AbstractSharedCrateClusterTest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;

public class SQLCrateClusterTest extends AbstractSharedCrateClusterTest {

    /**
     * Execute an SQL Statement on a random node of the cluster
     *
     * @param stmt the SQL Statement
     * @param args the arguments to replace placeholders ("?") in the statement
     * @return the SQLResponse
     */
    protected SQLResponse execute(String stmt, Object[] args) {
        return client().execute(SQLAction.INSTANCE, new SQLRequest(stmt, args)).actionGet();
    }

    /**
     * Execute an SQL Statement on a random node of the cluster
     *
     * @param stmt the SQL Statement
     * @return the SQLResponse
     */
    protected SQLResponse execute(String stmt) {
        return execute(stmt, new Object[0]);
    }

    /**
     * Get all mappings from an index as JSON String
     *
     * @param index the name of the index
     * @return the index mapping as String
     * @throws IOException
     */
    protected String getIndexMapping(String index) throws IOException {
        ClusterStateRequest request = Requests.clusterStateRequest()
                .filterRoutingTable(true)
                .filterNodes(true)
                .filteredIndices(index);
        ClusterStateResponse response = client().admin().cluster().state(request)
                .actionGet();

        MetaData metaData = response.getState().metaData();
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();

        IndexMetaData indexMetaData = metaData.iterator().next();
        for (MappingMetaData mappingMd : indexMetaData.mappings().values()) {
            builder.field(mappingMd.type());
            builder.map(mappingMd.sourceAsMap());
        }
        builder.endObject();

        return builder.string();
    }

    /**
     * Get the IndexSettings as JSON String
     *
     * @param index the name of the index
     * @return the IndexSettings as JSON String
     * @throws IOException
     */
    protected String getIndexSettings(String index) throws IOException {
        ClusterStateRequest request = Requests.clusterStateRequest()
                .filterRoutingTable(true)
                .filterNodes(true)
                .filteredIndices(index);
        ClusterStateResponse response = client().admin().cluster().state(request)
                .actionGet();

        MetaData metaData = response.getState().metaData();
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();

        for (IndexMetaData indexMetaData : metaData) {
            builder.startObject(indexMetaData.index(), XContentBuilder.FieldCaseConversion.NONE);
            builder.startObject("settings");
            Settings settings = indexMetaData.settings();
            for (Map.Entry<String, String> entry : settings.getAsMap().entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
            builder.endObject();

            builder.endObject();
        }

        builder.endObject();

        return builder.string();
    }

    /**
     * Execute an SQLRequest on a random client of the cluster like it would
     * be executed by an HTTP REST Request
     *
     * @param source the body of the statement, a JSON String containing the "stmt" and the "args"
     * @return the Response as JSON String
     * @throws IOException
     */
    protected String restSQLExecute(String source) throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.generator().usePrettyPrint();
        SQLRequestBuilder requestBuilder = new SQLRequestBuilder(client());
        requestBuilder.source(new BytesArray(source));
        SQLResponse response = requestBuilder.execute().actionGet();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        return builder.string();
    }
}
