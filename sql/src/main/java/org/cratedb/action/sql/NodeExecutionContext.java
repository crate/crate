package org.cratedb.action.sql;

import org.cratedb.action.parser.QueryPlanner;
import org.cratedb.sql.parser.parser.ColumnReference;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.mapper.object.RootObjectMapper;
import org.elasticsearch.indices.IndicesService;

import java.util.*;

public class NodeExecutionContext {

    private final IndicesService indicesService;
    private final ClusterService clusterService;
    private final QueryPlanner queryPlanner;
    public static final String DEFAULT_TYPE = "default";

    @Inject
    public NodeExecutionContext(IndicesService indicesService, ClusterService clusterService,
                                QueryPlanner queryPlanner) {
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.queryPlanner = queryPlanner;
    }

    public ITableExecutionContext tableContext(String schema, String table) {
        if (schema != null && schema.equalsIgnoreCase(InformationSchemaTableExecutionContext.SCHEMA_NAME)) {
              return new InformationSchemaTableExecutionContext(table);
        }
        DocumentMapper dm = indicesService.indexServiceSafe(table).mapperService()
            .documentMapper(DEFAULT_TYPE);
        MappingMetaData mappingMetaData = clusterService.state().metaData().index(table)
            .mappingOrDefault(DEFAULT_TYPE);
        if (dm!=null && mappingMetaData != null){
            return new TableExecutionContext(table, dm, mappingMetaData);
        }
        return null;
    }

    /**
     *
     * @param name the name of the table
     * @return the table
     */
    public ITableExecutionContext tableContext(String name) {
        return tableContext(null, name);
    }

    public QueryPlanner queryPlanner() {
        return queryPlanner;
    }
}
