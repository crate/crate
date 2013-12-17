package org.cratedb.action.collect.scope;

import org.cratedb.DataType;
import org.cratedb.sql.CrateException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.inject.Inject;

public class ClusterNameExpression extends ClusterLevelExpression<String> {

    public static final String NAME = "cluster.name";
    private final ClusterName clusterName;

    @Inject
    public ClusterNameExpression(ClusterName clusterName) {
        this.clusterName = clusterName;
    }

    @Override
    public String evaluate() throws CrateException {
        return clusterName.value();
    }

    @Override
    public DataType returnType() {
        return DataType.STRING;
    }

    @Override
    public ExpressionScope getScope() {
        return ExpressionScope.CLUSTER;
    }

    @Override
    public String getFullyQualifiedName() {
        return NAME;
    }
}
