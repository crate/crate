package org.cratedb.service;

import com.google.common.collect.ImmutableMap;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.action.sql.SQLResponse;
import org.cratedb.information_schema.*;
import org.cratedb.sql.SQLParseException;
import org.cratedb.sql.TableUnknownException;
import org.cratedb.sql.parser.parser.NodeTypes;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;

import static org.elasticsearch.action.support.PlainActionFuture.newFuture;

/**
 * the informationSchemaService creates and manages lucene in-memory indices which can be used
 * to query the clusterState (tables, columns... etc) using SQL.
 *
 * Each node holds and manages its own index.
 * They are created on first access and once created are
 * updated according to various events (like index created...)
 */
public class InformationSchemaService extends AbstractLifecycleComponent<InformationSchemaService> {

    private final ClusterService clusterService;
    private final Object readLock = new Object();
    private boolean dirty;
    private ClusterStateListener listener;
    protected final ESLogger logger;

    public static final ImmutableMap<String, InformationSchemaTable> tables = new ImmutableMap
            .Builder<String, InformationSchemaTable>()
            .put(TablesTable.NAME, new TablesTable())
            .put(TableConstraintsTable.NAME, new TableConstraintsTable())
            .put(TableColumnsTable.NAME, new TableColumnsTable())
            .put(TableIndicesTable.NAME, new TableIndicesTable())
            .build();

    @Inject
    public InformationSchemaService(Settings settings,
                                    ClusterService clusterService) {
        super(settings);
        this.clusterService = clusterService;
        this.dirty = false;
        logger = Loggers.getLogger(getClass(), settings);
    }

    @Override
    protected void doStart() throws ElasticSearchException {

        logger.info("starting...");
        listener = new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                if (event.metaDataChanged()) {
                    synchronized (readLock) {
                        dirty = true;
                    }
                }
            }
        };
        clusterService.add(listener);

    }

    @Override
    protected void doStop() throws ElasticSearchException {
        logger.info("stopping...");
        clusterService.remove(listener);
    }

    @Override
    protected void doClose() throws ElasticSearchException {
        for (ImmutableMap.Entry<String, InformationSchemaTable> tableEntry: this.tables.entrySet()) {
            tableEntry.getValue().close();
        }
    }

    public void execute(ParsedStatement stmt, final ActionListener<SQLResponse> listener) throws IOException {
        if (!stmt.schemaName().equalsIgnoreCase("information_schema")) {
            listener.onFailure(new IllegalStateException("Trying to query information schema with invalid ParsedStatement"));
            return;
        }

        if (stmt.nodeType() != NodeTypes.CURSOR_NODE) {
            throw new SQLParseException(
                "INFORMATION_SCHEMA tables are virtual and read-only. Only SELECT statements are supported");
        }

        InformationSchemaTable table = tables.get(stmt.tableName().toLowerCase());
        if (table == null) {
            listener.onFailure(new TableUnknownException(stmt.tableName()));
        } else {
            // reindex if dirty
            synchronized (readLock) {
                ClusterState state = clusterService.state();
                if (dirty) {
                    for (InformationSchemaTable informationSchemaTable: tables.values()) {
                        informationSchemaTable.index(state);
                    }
                    dirty = false;
                } else if (!table.initialized()) {
                    // prefill table if cluster state is not dirty (e.g. first query)
                    table.index(state);
                }
            }
            table.query(stmt, listener);
        }

    }

    public ActionFuture<SQLResponse> execute(ParsedStatement stmt) throws IOException {
        PlainActionFuture<SQLResponse> future = newFuture();
        execute(stmt, future);
        return future;
    }

}
