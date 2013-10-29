package org.cratedb.action.sql;

import org.cratedb.action.sql.analyzer.AnalyzerService;
import org.cratedb.action.parser.QueryPlanner;
import org.cratedb.sql.TableUnknownException;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.IndicesService;

public class NodeExecutionContext {

    private final IndicesService indicesService;
    private final ClusterService clusterService;
    private final AnalyzerService analyzerService;
    private final QueryPlanner queryPlanner;
    public static final String DEFAULT_TYPE = "default";
 
    @Inject
    public NodeExecutionContext(IndicesService indicesService,
                                ClusterService clusterService,
                                AnalyzerService analyzerService,
                                
                                QueryPlanner queryPlanner) {
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.analyzerService = analyzerService;
        this.queryPlanner = queryPlanner;
    }

    public ITableExecutionContext tableContext(String schema, String table) {
        if (schema != null && schema.equalsIgnoreCase(InformationSchemaTableExecutionContext.SCHEMA_NAME)) {
              return new InformationSchemaTableExecutionContext(table);
        }

        // TODO: remove documentMapper
        // the documentMapper isn't available on nodes that don't contain the index.
        DocumentMapper dm;
        try {
            dm = indicesService.indexServiceSafe(table).mapperService().documentMapper(DEFAULT_TYPE);
        } catch (IndexMissingException ex) {
            throw new TableUnknownException(table, ex);
        }

        IndexMetaData indexMetaData = clusterService.state().metaData().index(table);
        if (dm != null && indexMetaData != null){
            return new TableExecutionContext(table, indexMetaData.mappingOrDefault(DEFAULT_TYPE), dm);
        } else if (indexMetaData != null) {
            return new TableExecutionContext(table, indexMetaData.mappingOrDefault(DEFAULT_TYPE));
        }

        return null;
    }

    public QueryPlanner queryPlanner() {
        return queryPlanner;
    }

    public AnalyzerService analyzerService() {
        return analyzerService;
    }
}
