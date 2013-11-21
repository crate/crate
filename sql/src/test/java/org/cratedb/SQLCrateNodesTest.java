package org.cratedb;

import org.cratedb.action.sql.SQLAction;
import org.cratedb.action.sql.SQLRequest;
import org.cratedb.action.sql.SQLResponse;
import org.cratedb.test.integration.AbstractCrateNodesTests;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;

public class SQLCrateNodesTest extends AbstractCrateNodesTests {

    public SQLResponse execute(Client client, String stmt, Object[]  args) {
        return client.execute(SQLAction.INSTANCE, new SQLRequest(stmt, args)).actionGet();
    }

    public SQLResponse execute(Client client, String stmt) {
        return execute(client, stmt, new Object[0]);
    }

    public SQLResponse execute(String stmt, Object[] args) {
        return execute(client(), stmt, args);
    }

    public SQLResponse execute(String stmt) {
        return execute(stmt, new Object[0]);
    }

    public void createIndex(String indexName, Settings indexSettings, XContentBuilder mappingBuilder) {
        super.createIndex(client(), indexName, indexSettings, Constants.DEFAULT_MAPPING_TYPE, mappingBuilder);
    }

    public void createIndex(Client client, String indexName, Settings indexSettings, XContentBuilder mappingBuilder) {
        super.createIndex(client, indexName, indexSettings, Constants.DEFAULT_MAPPING_TYPE, mappingBuilder);
    }
}
