package org.cratedb.action.sql;

import org.cratedb.action.parser.QueryPlanner;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.inject.Inject;


public class NodeExecutionContext {

    private final ClusterService clusterService;
    private final QueryPlanner queryPlanner;
    public static final String DEFAULT_TYPE = "default";

    @Inject
    public NodeExecutionContext(ClusterService clusterService, QueryPlanner queryPlanner) {
        this.clusterService = clusterService;
        this.queryPlanner = queryPlanner;
    }

    public ITableExecutionContext tableContext(String schema, String table) {
        if (schema != null && schema.equalsIgnoreCase(InformationSchemaTableExecutionContext.SCHEMA_NAME)) {
              return new InformationSchemaTableExecutionContext(table);
        }

        IndexMetaData indexMetaData = clusterService.state().metaData().index(table);
        if (indexMetaData != null){
            return new TableExecutionContext(table, indexMetaData.mappingOrDefault(DEFAULT_TYPE));
        }

        return null;
    }

    public QueryPlanner queryPlanner() {
        return queryPlanner;
    }
}
