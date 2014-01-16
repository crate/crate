 
package io.crate.sql.tree;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class Explain
        extends Statement
{
    private final Statement statement;
    private final List<ExplainOption> options;

    public Explain(Statement statement, List<ExplainOption> options)
    {
        this.statement = checkNotNull(statement, "statement is null");
        if (options == null) {
            this.options = ImmutableList.of();
        }
        else {
            this.options = ImmutableList.copyOf(options);
        }
    }

    public Statement getStatement()
    {
        return statement;
    }

    public List<ExplainOption> getOptions()
    {
        return options;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitExplain(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(statement, options);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        Explain o = (Explain) obj;
        return Objects.equal(statement, o.statement) &&
                Objects.equal(options, o.options);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("statement", statement)
                .add("options", options)
                .toString();
    }
}
