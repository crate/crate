package io.crate.sql.tree;


public class SubscriptExpression extends Expression {

    private Expression name;
    private Expression index;

    public SubscriptExpression(Expression nameExpression, Expression indexExpression) {
        this.name = nameExpression;
        this.index = indexExpression;
    }

    public Expression name() {
        return name;
    }

    public Expression index() {
        return index;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitSubscriptExpression(this, context);
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (index != null ? index.hashCode() : 0);
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SubscriptExpression that = (SubscriptExpression) o;

        if (index != null ? !index.equals(that.index) : that.index != null) return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;

        return true;
    }
}
