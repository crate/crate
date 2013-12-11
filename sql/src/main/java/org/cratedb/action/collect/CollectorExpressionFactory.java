package org.cratedb.action.collect;

import com.google.common.collect.ImmutableMap;
import org.cratedb.action.sql.ITableExecutionContext;
import org.cratedb.sql.parser.parser.NodeTypes;
import org.cratedb.sql.parser.parser.ValueNode;

import java.util.Map;

public interface CollectorExpressionFactory {

    public Expression create(ValueNode node, ITableExecutionContext tec);

}
