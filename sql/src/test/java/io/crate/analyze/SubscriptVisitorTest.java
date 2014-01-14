package io.crate.analyze;

import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.Expression;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class SubscriptVisitorTest {

    public SubscriptVisitor visitor = new SubscriptVisitor();

    @Test
    public void testVisitSubscriptExpression() throws Exception {
        SubscriptContext context = new SubscriptContext();
        Expression expression = SqlParser.createExpression("a['x']['y']");
        expression.accept(visitor, context);

        assertEquals("a", context.column());
        assertEquals("x", context.parts().get(0));
        assertEquals("y", context.parts().get(1));
    }

    @Test (expected = IllegalArgumentException.class)
    public void testInvalidSubscriptExpressionName() throws Exception {
        SubscriptContext context = new SubscriptContext();
        Expression expression = SqlParser.createExpression("'a'['x']['y']");
        expression.accept(visitor, context);
    }

    @Test (expected = IllegalArgumentException.class)
    public void testInvalidSubscriptExpressionIndex() throws Exception {
        SubscriptContext context = new SubscriptContext();
        Expression expression = SqlParser.createExpression("a[x]['y']");
        expression.accept(visitor, context);
    }
}
