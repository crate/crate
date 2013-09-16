package org.cratedb.action.parser;

import com.akiban.sql.StandardException;
import com.akiban.sql.parser.Visitor;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.util.List;

/**
 * The XContentVisitor is an extended Visitor interface provided by the akiban SQL-Parser
 * See https://github.com/akiban/sql-parser for more information.
 *
 * Implementations purpose is generating XContent from SQL statements(Akiban nodes).
 *
 */
public interface XContentVisitor extends Visitor {

    public XContentBuilder getXContentBuilder() throws StandardException;

    public List<String> getIndices();

    /**
     * See {@link org.cratedb.action.parser.XContentGenerator#outputFields()}
     * @return
     */
    public List<Tuple<String, String>> outputFields();

}
