package org.cratedb.stubs;

import org.cratedb.Constants;
import org.cratedb.DataType;
import org.cratedb.action.groupby.aggregate.AggFunction;
import org.cratedb.action.groupby.aggregate.count.CountAggFunction;
import org.cratedb.action.groupby.aggregate.max.MaxAggFunction;
import org.cratedb.action.groupby.aggregate.min.MinAggFunction;
import org.cratedb.action.groupby.aggregate.sum.SumAggFunction;
import org.cratedb.action.parser.QueryPlanner;
import org.cratedb.action.sql.NodeExecutionContext;
import org.cratedb.action.sql.TableExecutionContext;
import org.cratedb.index.IndexMetaDataExtractor;
import org.cratedb.sql.types.*;
import org.cratedb.test.integration.PathAccessor;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * this class contains mocks and/or stubs which can be used for unit-testing.
 */
public class HitchhikerMocks {

    // maps that are usually created by the injector.
    public static Map<String, AggFunction> aggFunctionMap = new HashMap<String, AggFunction>() {{
        put(CountAggFunction.COUNT_ROWS_NAME, new CountAggFunction());
        put(CountAggFunction.NAME, new CountAggFunction());
        put(MinAggFunction.NAME, new MinAggFunction());
        put(MaxAggFunction.NAME, new MaxAggFunction());
        put(SumAggFunction.NAME, new SumAggFunction());
    }};
    public static Map<DataType, SQLType> sqlTypes = new HashMap<DataType, SQLType>() {{
        put(DataType.BOOLEAN, new BooleanSQLType());
        put(DataType.STRING, new StringSQLType());
        put(DataType.BYTE, new ByteSQLType());
        put(DataType.SHORT, new ShortSQLType());
        put(DataType.INTEGER, new IntegerSQLType());
        put(DataType.LONG, new LongSQLType());
        put(DataType.FLOAT, new FloatSQLType());
        put(DataType.DOUBLE, new DoubleSQLType());
        put(DataType.TIMESTAMP, new TimeStampSQLType());
        put(DataType.CRATY, new CratySQLType());
        put(DataType.IP, new IpSQLType());
    }};


    public static NodeExecutionContext nodeExecutionContext() throws IOException {

        Settings settings = ImmutableSettings.builder()
            .put(QueryPlanner.SETTINGS_OPTIMIZE_PK_QUERIES, true)
            .build();
        QueryPlanner queryPlanner = new QueryPlanner(settings);

        NodeExecutionContext context = mock(NodeExecutionContext.class);
        when(context.queryPlanner()).thenReturn(queryPlanner);
        when(context.availableAggFunctions()).thenReturn(aggFunctionMap);

        for (TableContextPair tableContextPair : tableContexts()) {
            when(context.tableContext(tableContextPair.schema, tableContextPair.tableName))
                .thenReturn(tableContextPair.context);
        }

        return context;
    }

    private static List<TableContextPair> tableContexts() throws IOException {
        List<TableContextPair> result = new ArrayList<>();

        // extend as required.
        result.add(new TableContextPair(null, charactersContext()));

        return result;
    }

    private static TableExecutionContext charactersContext() throws IOException {
        IndexMetaData indexMetaData = getIndexMetaData("characters");
        IndexMetaDataExtractor metaDataExtractor = new IndexMetaDataExtractor(indexMetaData);
        return new TableExecutionContext(
            "characters",
            metaDataExtractor,
            new SQLFieldMapper(sqlTypes, metaDataExtractor),
            false
        );
    }

    public static IndexMetaData getIndexMetaData(String tableName) throws IOException {
        byte[] bytes = PathAccessor.bytesFromPath(
            String.format("/essetup/mappings/%s.json", tableName),
            HitchhikerMocks.class);
        Settings settings = ImmutableSettings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .build();

        Map<String, Object> mappingSource = XContentHelper.convertToMap(bytes, true).v2();
        return IndexMetaData.builder(tableName)
            .settings(settings)
            .putMapping(new MappingMetaData(Constants.DEFAULT_MAPPING_TYPE, mappingSource))
            .build();
    }

    static class TableContextPair {
        private final TableExecutionContext context;
        private final String schema;
        private final String tableName;

        public TableContextPair(String schema, TableExecutionContext context) {
            this.schema = schema;
            this.tableName = context.tableName;
            this.context = context;
        }
    }
}
