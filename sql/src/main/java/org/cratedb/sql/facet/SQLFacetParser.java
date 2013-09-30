package org.cratedb.sql.facet;

import org.cratedb.action.sql.NodeExecutionContext;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.action.sql.parser.SQLXContentSourceContext;
import org.cratedb.action.sql.parser.SQLXContentSourceParser;
import org.cratedb.sql.parser.StandardException;
import org.elasticsearch.action.update.TransportUpdateAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.FacetParser;
import org.elasticsearch.search.facet.FacetPhaseExecutionException;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;


/**
 *
 */
public class SQLFacetParser extends AbstractComponent implements FacetParser {

    private final NodeExecutionContext executionContext;
    private final TransportUpdateAction updateAction;

    @Inject
    public SQLFacetParser(
            Settings settings,
            NodeExecutionContext executionContext,
            TransportUpdateAction updateAction) {
        super(settings);
        InternalSQLFacet.registerStreams();
        this.executionContext = executionContext;
        this.updateAction = updateAction;
    }

    @Override
    public String[] types() {
        return new String[]{
                SQLFacet.TYPE
        };
    }

    @Override
    public FacetExecutor.Mode defaultMainMode() {
        return FacetExecutor.Mode.COLLECTOR;
    }

    @Override
    public FacetExecutor.Mode defaultGlobalMode() {
        return FacetExecutor.Mode.COLLECTOR;
    }

    @Override
    public FacetExecutor parse(String facetName, XContentParser parser,
            SearchContext searchContext) throws IOException {
        SQLXContentSourceContext context = new SQLXContentSourceContext();
        SQLXContentSourceParser sqlParser = new SQLXContentSourceParser(context);
        try {
            sqlParser.parse(parser);
        } catch (Exception e) {
            throw new FacetPhaseExecutionException(facetName, "body parse failure", e);
        }
        ParsedStatement stmt = null;
        try {
            stmt = new ParsedStatement(context.stmt(), context.args(), executionContext);
        } catch (StandardException e) {
            throw new FacetPhaseExecutionException(facetName, "sql parse failure", e);
        }

        return new SQLFacetExecutor(stmt, searchContext, updateAction);
    }

}
