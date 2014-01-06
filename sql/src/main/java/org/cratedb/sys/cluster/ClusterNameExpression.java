package org.cratedb.sys.cluster;

import org.cratedb.DataType;
import org.cratedb.sys.Scope;
import org.cratedb.sys.ScopedName;
import org.cratedb.sys.SysExpression;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.inject.Inject;

public class ClusterNameExpression implements SysExpression<String> {

    private final ClusterName clusterName;
    public static final ScopedName NAME = new ScopedName(Scope.CLUSTER, "name");

    @Inject
    public ClusterNameExpression(ClusterName clusterName) {
        this.clusterName = clusterName;
    }

    @Override
    public String evaluate() {
        return clusterName.value();
    }

    @Override
    public DataType returnType() {
        return DataType.STRING;
    }

}
