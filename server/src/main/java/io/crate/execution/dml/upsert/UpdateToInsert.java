
package io.crate.execution.dml.upsert;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import io.crate.data.Input;
import io.crate.execution.dml.IndexItem;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.expression.BaseImplementationSymbolVisitor;
import io.crate.expression.reference.Doc;
import io.crate.expression.reference.DocRefResolver;
import io.crate.expression.reference.ReferenceResolver;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocTableInfo;

/**
 * Uses a stored document to convert an UPDATE into an absolute INSERT
 * (or the structure required for indexing)
 *
 * <p>
 * Examples:
 * </p>
 *
 * <pre>
 *  Table with (x, y, z), existing record: (1, 2, 3)
 *
 *  UPDATE tbl SET
 *      x = x + 2,
 *      y = 42
 *
 *  Result:
 *      targetColumns: [x,  y, z]
 *      values:        [3, 42, 3]
 * </pre>
 *
 * <pre>
 *  Table with (x, o (y, z)). Existing record (1, {y=10, z=20})
 *
 *  UPDATE tbl SET
 *      o['y'] = 40
 *
 *  Result:
 *      targetColumns [x,            o]
 *      values:       [1, {y=40, z=20}]
 *  </pre>
 *
 *
 *  <pre>
 *   Table with (x, y, z), existing record: (1, 2, 3)
 *
 *   INSERT INTO tbl (x, y, z) values (1, 10, 20)
 *   ON CONFLICT (x) DO UPDATE SET
 *      y = y + excluded.y
 *
 *  Result:
 *      targetColumns: [x,  y, z]
 *      values:        [1, 12, 3]
 **/
public final class UpdateToInsert {

    private final DocTableInfo table;
    private final Evaluator eval;
    private final List<Reference> updateColumns;

    record Values(Doc doc, Object[] excludedValues) {
    }

    private static class Evaluator extends BaseImplementationSymbolVisitor<Values> {

        private final ReferenceResolver<CollectExpression<Doc, ?>> refResolver;

        private Evaluator(NodeContext nodeCtx,
                          TransactionContext txnCtx,
                          ReferenceResolver<CollectExpression<Doc, ?>> refResolver) {
            super(txnCtx, nodeCtx);
            this.refResolver = refResolver;
        }

        @Override
        public Input<?> visitInputColumn(InputColumn inputColumn, Values context) {
            return Literal.ofUnchecked(inputColumn.valueType(), context.excludedValues[inputColumn.index()]);
        }

        @Override
        public Input<?> visitReference(Reference symbol, Values values) {
            CollectExpression<Doc, ?> expr = refResolver.getImplementation(symbol);
            expr.setNextRow(values.doc);
            return expr;
        }
    }

    public UpdateToInsert(NodeContext nodeCtx,
                          TransactionContext txnCtx,
                          DocTableInfo table,
                          String[] updateColumns) {
        var refResolver = new DocRefResolver(table.partitionedBy());
        this.table = table;
        this.eval = new Evaluator(nodeCtx, txnCtx, refResolver);
        this.updateColumns = new ArrayList<>(updateColumns.length);
        for (String columnName : updateColumns) {
            ColumnIdent column = ColumnIdent.fromPath(columnName);
            Reference existingRef = table.getReference(column);
            Reference reference = existingRef == null
                ? table.getDynamic(column, true, txnCtx.sessionSettings().errorOnUnknownObjectKey())
                : existingRef;
            this.updateColumns.add(reference);
        }
    }

    public IndexItem convert(Doc doc, Symbol[] updateAssignments, Object[] excludedValues) {
        Object[] insertValues = new Object[table.columns().size()];
        Values values = new Values(doc, excludedValues);

        Iterator<Reference> it = table.columns().iterator();
        int i = 0;
        while (it.hasNext()) {
            Reference ref = it.next();
            int updateIdx = updateColumns.indexOf(ref);
            if (updateIdx >= 0) {
                Symbol symbol = updateAssignments[updateIdx];
                Object value = symbol.accept(eval, values).value();
                assert ref.column().isTopLevel()
                    : "If updateColumns.indexOf(reference-from-table.columns()) is >= 0 it must be a top level reference";
                insertValues[i] = value;
            } else {
                insertValues[i] = ref.accept(eval, values).value();
            }
            i++;
        }
        return new IndexItem.Item(
            doc.getId(),
            List.of(),
            insertValues,
            doc.getSeqNo(),
            doc.getPrimaryTerm()
        );
    }
}
