package org.cratedb.information_schema;

import com.google.common.collect.ImmutableMap;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.action.sql.SQLResponse;
import org.cratedb.sql.CrateException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;

public interface InformationSchemaTable {
    public Iterable<String> cols();
    public ImmutableMap<String, InformationSchemaColumn> fieldMapper();

    /**
     * initialize this table and its resources
     */
    public void init() throws CrateException;
    public boolean initialized();

    /**
     * close this table and release its resources
     * @throws CrateException
     */
    public void close() throws CrateException;

    /**
     * Query this table for results
     * @param stmt the statement containing the parsed query
     * @param listener the listener to be called on success or failure
     */
    public void query(ParsedStatement stmt, ActionListener<SQLResponse> listener, long requestStartedTime);

    /**
     * extract and index table data from the current ClusterState
     *
     * not synchronized, please synchronize when using to stay threadsafe
     * @param clusterState
     */
    public void index(ClusterState clusterState);
}
