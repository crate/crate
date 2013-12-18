package org.cratedb.service;

import org.cratedb.action.collect.scope.GlobalExpressionDescription;
import org.cratedb.action.collect.scope.ScopedExpression;
import org.elasticsearch.common.inject.Inject;

import java.util.Map;

public class GlobalExpressionService {

    private final Map<String, ScopedExpression> globalExpressions;

    @Inject
    public GlobalExpressionService(Map<String, ScopedExpression> globalExpressions) {
        this.globalExpressions = globalExpressions;
    }

    public boolean expressionExists(String fullyQualifiedName) {
        return globalExpressions.containsKey(fullyQualifiedName);
    }

    public GlobalExpressionDescription getDescription(String fqdn) {
        ScopedExpression<?> expr = getExpression(fqdn);
        if (expr == null) {
            return null;
        } else {
            return new GlobalExpressionDescription(expr);
        }
    }

    public ScopedExpression<?> getExpression(String fqdn) {
        return globalExpressions.get(fqdn);
    }
}
