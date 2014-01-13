package io.crate.sql.tree;


public class ParameterExpression extends Expression{

    private final int position;

    public ParameterExpression(int position) {
        this.position = position;
    }


    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitParameterExpression(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ParameterExpression that = (ParameterExpression) o;

        if (position != that.position) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return position;
    }

    public int position() {
        return position;
    }
}
