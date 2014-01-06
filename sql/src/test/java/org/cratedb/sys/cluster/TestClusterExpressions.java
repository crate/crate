package org.cratedb.sys.cluster;

import org.cratedb.action.collect.Expression;
import org.cratedb.sys.Scope;
import org.cratedb.sys.ScopedName;
import org.cratedb.sys.SysExpression;
import org.cratedb.sys.SysExpressionModule;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.inject.*;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestClusterExpressions {

    final TypeLiteral<Map<ScopedName, SysExpression>> expressionsMap = new TypeLiteral<Map<ScopedName, SysExpression>>() {
    };

    class TestModule extends AbstractModule {

        @Override
        protected void configure() {
            bind(ClusterName.class).toInstance(new ClusterName("mega_crate"));
        }
    }

    final TestModule testModule = new TestModule();

    @Test
    public void testClusterName() throws Exception {
        Injector i = new ModulesBuilder().add(testModule, new SysExpressionModule()).createInjector();
        Map<ScopedName, SysExpression> expressions = i.getInstance(Key.get(expressionsMap));
        Expression e = expressions.get(new ScopedName(Scope.CLUSTER, "name"));
        assertEquals("mega_crate", e.evaluate());
    }
}
