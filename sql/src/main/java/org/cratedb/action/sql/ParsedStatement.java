package org.cratedb.action.sql;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.lucene.search.Query;
import org.cratedb.DataType;
import org.cratedb.action.collect.ColumnReferenceExpression;
import org.cratedb.action.collect.Expression;
import org.cratedb.action.collect.scope.ScopedExpression;
import org.cratedb.action.groupby.aggregate.AggExpr;
import org.cratedb.action.parser.ColumnDescription;
import org.cratedb.service.SQLParseService;
import org.cratedb.sql.parser.parser.NodeTypes;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import java.util.*;

public class ParsedStatement {

    private ESLogger logger = Loggers.getLogger(ParsedStatement.class);

    public final ArrayList<Tuple<String, String>> outputFields = new ArrayList<>();

    private String schemaName;
    private String virtualTableName;
    private String[] indices = null;

    private ActionType type;
    private int nodeType;

    private Map<String, Object> updateDoc;

    public boolean versionSysColumnSelected = false;

    public List<IndexRequest> indexRequests;

    public Long versionFilter;
    public Double scoreMinimum;
    public String stmt;
    public Query query;

    public String importPath;

    public boolean tableNameIsAlias = false;

    /**
     * used for create analyzer statements
     */
    public Settings createAnalyzerSettings = ImmutableSettings.EMPTY;

    /**
     * set if the where clause contains a single pk column.
     * E.g.:
     *      pk_col = 1
     */
    public String primaryKeyLookupValue;

    /**
     * set if the where clause contains multiple pk columns.
     * E.g.:
     *      pk_col = 1 or pk_col = 2
     */
    public Set<String> primaryKeyValues = new HashSet<>();

    public Set<String> routingValues = new HashSet<>();

    public Set<String> columnsWithFilter = new HashSet<>();
    public int orClauses = 0;
    public List<OrderByColumnName> orderByColumns = new ArrayList<>();
    private Optional<SeenValueContext> seenValueContext = Optional.absent();

    public String[] getRoutingValues() {
        return routingValues.toArray(new String[routingValues.size()]);
    }

    public ParsedStatement(String stmt) {
        this.stmt = stmt;
    }

    public ImmutableMap<String, Object> indexSettings;
    public ImmutableMap<String, Object> indexMapping;

    public boolean isInformationSchemaQuery() {
        return schemaName() != null && schemaName().equalsIgnoreCase("information_schema");
    }

    /**
     * returns true if this statement will result in a global aggregate operation
     */
    public boolean isGlobalAggregate() {
        return aggregateExpressions().size() > 0 &&
                !countRequest() &&
                !hasGroupBy();
    }

    public boolean isStatsQuery() {
        return schemaName() != null && schemaName().equalsIgnoreCase("stats");
    }

    /**
     * returns true if this statement is executed on a virtual table
     * that is not backed by an elasticsearch index like
     * stats and information_schema tables
     */
    public boolean hasVirtualTable() {
        return isStatsQuery() || isInformationSchemaQuery();
    }

    public static enum ActionType {
        SEARCH_ACTION,
        INSERT_ACTION,
        DELETE_BY_QUERY_ACTION,
        BULK_ACTION, GET_ACTION,
        DELETE_ACTION,
        UPDATE_ACTION,
        CREATE_INDEX_ACTION,
        DELETE_INDEX_ACTION,
        MULTI_GET_ACTION,
        INFORMATION_SCHEMA,
        CREATE_ANALYZER_ACTION,
        COPY_IMPORT_ACTION,
        STATS
    }

    public static final int UPDATE_RETRY_ON_CONFLICT = 3;

    public BytesReference xcontent;

    private List<Expression> groupByExpressions = new ArrayList<>();
    private ImmutableSet<String> groupByColumnNames = ImmutableSet.of();
    private List<ColumnDescription> resultColumnList;

    public List<ColumnDescription> resultColumnList() {
        return resultColumnList;
    }

    public ParsedStatement resultColumnList(List<ColumnDescription> resultColumnList) {
        this.resultColumnList = resultColumnList;
        return this;
    }

    private List<AggExpr> aggregateExpressions;
    @SuppressWarnings("unchecked")
    public List<AggExpr> aggregateExpressions() {
        if (aggregateExpressions == null) {
            return Collections.EMPTY_LIST;
        } else {
            return aggregateExpressions;
        }
    }

    public ParsedStatement aggregateExpressions(List<AggExpr> aggregateExpressions) {
        this.aggregateExpressions = aggregateExpressions;
        return this;
    }

    public boolean hasCountStarAggregate = false;
    public boolean hasStoppableAggregate = false; // true if any aggregate is able to terminate collection earlier
    public boolean hasDistinctAggregate = false;

    // If -1, reducer count is evaluated dynamically
    public Integer partialReducerCount = -1;

    private List<ScopedExpression<?>> globalExpressions = new ArrayList<>();
    @SuppressWarnings("unchecked")
    public List<ScopedExpression<?>> globalExpressions() {
        return globalExpressions;
    }
    public ParsedStatement addGlobalExpressionSafe(ScopedExpression<?> expression) {
        if (globalExpressions == null) {
            globalExpressions = new ArrayList<>();
        }
        this.globalExpressions.add(expression);
        return this;
    }
    public int globalExpressionCount() {
        return globalExpressions.size();
    }

    private Integer limit = null;
    private Integer offset = null;

    public void offset(Integer offset) {
        this.offset = offset;
    }

    public int offset() {
        if (offset == null) {
            return 0;
        }
        return offset;
    }

    public void limit(Integer limit) {
        this.limit = limit;
    }

    public int limit() {
        if (limit == null) {
            return SQLParseService.DEFAULT_SELECT_LIMIT;
        }
        return limit;
    }

    public int totalLimit() {
        return limit() + offset();
    }

    public List<OrderByColumnIdx> orderByIndices;
    public OrderByColumnIdx[] orderByIndices() {
        if (orderByIndices != null) {
            return orderByIndices.toArray(new OrderByColumnIdx[orderByIndices.size()]);
        }

        return new OrderByColumnIdx[0];
    }

    public void schemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String schemaName() {
        return schemaName;
    }

    /**
     * TableName of hardcoded virtual tables e.g. `shards` table of the `stats` schema
     *
     * @param virtualTableName
     */
    public void virtualTableName(String virtualTableName) {
        this.virtualTableName = virtualTableName;
    }

    /**
     * TableName of hardcoded virtual tables e.g. `shards` table of the `stats` schema
     *
     * @return
     */
    public String virtualTableName() {
        return virtualTableName;
    }

    public void tableName(String tableName) {
        if (indices == null) {
            indices = new String[] { tableName };
        } else {
            indices[0] = tableName;
        }
    }

    public String tableName() {
        return indices == null ? null : indices[0];
    }

    public String[] indices() {
        return indices;
    }

    public void addIndex(String index) {
        if (indices == null) {
            indices = new String[] { index };
        } else {
            List<String> list = Arrays.asList(indices);
            list.add(index);
            indices = list.toArray(new String[list.size()]);
        }
    }

    public void type(ActionType type) {
        this.type = type;
    }

    public ActionType type() {
        return type;
    }

    public void nodeType(int nodeType) {
        this.nodeType = nodeType;
    }

    public int nodeType() {
        return nodeType;
    }

    /**
     * Get the result column-names as listed in the SELECT Statement,
     * eventually including aliases, not real column-names
     * @return Array of Column-Name or -Alias Strings
     */
    public String[] cols() {
        String[] cols = new String[outputFields.size()];
        for (int i = 0; i < outputFields.size(); i++) {
            cols[i] = outputFields.get(i).v1();
        }
        return cols;
    }

    /**
     * Get the ColumnNames that are actually fetched from the table for this statement
     * @return Array of Column-Name Strings
     */
    public String[] columnNames() {
        String[] colNames = new String[outputFields.size()];
        for (int i=0; i < outputFields.size(); i++) {
            colNames[i] = outputFields.get(i).v2();
        }
        return colNames;
    }

    /**
     * @return boolean indicating if a facet is used to gather the result
     */
    public boolean useFacet() {
        // currently only the update statement uses facets
        return nodeType() == NodeTypes.UPDATE_NODE;
    }

    public Map<String, Object> updateDoc() {
        return updateDoc;
    }

    /**
     * returns the requested output fields as a list of tuples where
     * the left side is the alias and the right side is the column name
     *
     * @return list of tuples
     */
    public List<Tuple<String, String>> outputFields() {
        return outputFields;
    }

    /**
     * Adds an additional output field
     * @param alias the name under which the field will show up in the result
     * @param columnName the name of the column the value comes from
     */
    public void addOutputField(String alias, String columnName) {
        this.outputFields.add(new Tuple<>(alias, columnName));
    }

    public void updateDoc(Map<String, Object> updateDoc) {
        this.updateDoc = updateDoc;
    }

    public void hasCountStarAggregate(boolean hasCountStarAggregate) {
        this.hasCountStarAggregate = hasCountStarAggregate;
    }

    /**
     * returns true if this Statement can be executed by an ES CountRequest,
     * if it hast only one count star aggregate and is no group by query
     *
     * only works on "normal" tables.
     *
     */
    public boolean countRequest() {
        return !hasVirtualTable() && !hasGroupBy() && hasCountStarAggregate && aggregateExpressions().size() == 1;
    }

    public boolean hasGroupBy() {
        return (groupByExpressions != null && groupByExpressions.size() > 0);
    }

    public boolean hasOrderBy() {
        return (orderByIndices != null && !orderByIndices.isEmpty()) || !orderByColumns.isEmpty();
    }
    @SuppressWarnings("unchecked")
    public List<Expression> groupByExpressions() {
        return groupByExpressions;
    }

    public void groupByExpressions(List<Expression> groupByExpressions) {
        this.groupByExpressions = groupByExpressions;
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        for (Expression e : groupByExpressions) {
            if (e instanceof ScopedExpression) {
                builder.add(((ScopedExpression) e).name());
            } else if (e instanceof ColumnReferenceExpression) {
                builder.add(((ColumnReferenceExpression) e).columnName());
            }
        }
        groupByColumnNames = builder.build();
    }

    public ImmutableSet<String> groupByColumnNames() {
        return groupByColumnNames;
    }

    public synchronized SeenValueContext seenValueContext() {
        if (!seenValueContext.isPresent()) {
            seenValueContext = Optional.of(new SeenValueContext(aggregateExpressions));
        }
        return seenValueContext.get();
    }

    public DataType.Streamer[] getGroupKeyStreamers() {
        DataType.Streamer[] keyStreamers = new DataType.Streamer[
                groupByExpressions().size()];
        for (int i = 0; i < keyStreamers.length; i++) {
            keyStreamers[i] = groupByExpressions().get(i).returnType().streamer();
        }
        return keyStreamers;
    }
}
