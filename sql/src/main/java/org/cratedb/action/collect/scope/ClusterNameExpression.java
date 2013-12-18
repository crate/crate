package org.cratedb.action.collect.scope;

import org.apache.lucene.util.BytesRef;
import org.cratedb.DataType;
import org.cratedb.sql.CrateException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.inject.Inject;

public class ClusterNameExpression extends ClusterLevelExpression<BytesRef> {

    public static final String NAME = "sys.cluster.name";

    private final BytesRef clusterName;

    @Inject
    public ClusterNameExpression(ClusterName clusterName) {
        this.clusterName = new BytesRef(clusterName.value().getBytes());
    }

    @Override
    public BytesRef evaluate() throws CrateException {
        return clusterName;
    }

    @Override
    public DataType returnType() {
        return DataType.STRING;
    }

    @Override
    public String name() {
        return NAME;
    }
}
