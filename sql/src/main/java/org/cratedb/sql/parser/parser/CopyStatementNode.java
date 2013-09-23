/**
 * Copyright 2011-2013 Akiban Technologies, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.cratedb.sql.parser.parser;

import org.cratedb.sql.parser.StandardException;

/**
 * An CopyStatementNode represents the COPY command.
 *
 */

public class CopyStatementNode extends StatementNode
{
    public static enum Mode { TO_TABLE, FROM_TABLE, FROM_SUBQUERY };
    public static enum Format { CSV, MYSQL_DUMP };
    private Mode mode;
    private TableName tableName;
    private ResultColumnList columnList;
    private SubqueryNode subquery;
    private String filename;
    private Format format;
    private String delimiter, nullString, quote, escape, encoding;
    private boolean header;
    private long commitFrequency;
    
    /**
     * Initializer for an CopyStatementNode
     *
     * @param mode The copy direction.
     * @param subquery The derived table.
     * @param filename The file name or <code>null</code>.
     */

    public void init(Object mode, Object subquery, Object filename) {
        this.mode = (Mode)mode;
        this.subquery = (SubqueryNode)subquery;
        this.filename = (String)filename;
    }

    /**
     * Initializer for an CopyStatementNode
     *
     * @param mode The copy direction.
     * @param tableName The table name.
     * @param columnList The list of columns.
     * @param filename The file name or <code>null</code>.
     */

    public void init(Object mode, Object tableName, Object columnList, Object filename) {
        this.mode = (Mode)mode;
        this.tableName = (TableName)tableName;
        this.columnList = (ResultColumnList)columnList;
        this.filename = (String)filename;
    }

    public Mode getMode() {
        return mode;
    }
    public SubqueryNode getSubquery() {
        return subquery;
    }
    public TableName getTableName() {
        return tableName;
    }
    public ResultColumnList getColumnList() {
        return columnList;
    }
    public String getFilename() {
        return filename;
    }

    public Format getFormat() {
        return format;
    }
    public void setFormat(Format format) {
        this.format = format;
    }
    public String getDelimiter() {
        return delimiter;
    }
    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }
    public String getNullString() {
        return nullString;
    }
    public void setNullString(String nullString) {
        this.nullString = nullString;
    }
    public boolean isHeader() {
        return header;
    }
    public void setHeader(boolean header) {
        this.header = header;
    }
    public String getQuote() {
        return quote;
    }
    public void setQuote(String quote) {
        this.quote = quote;
    }
    public String getEscape() {
        return escape;
    }
    public void setEscape(String escape) {
        this.escape = escape;
    }
    public String getEncoding() {
        return encoding;
    }
    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }
    public long getCommitFrequency() {
        return commitFrequency;
    }
    public void setCommitFrequency(long commitFrequency) {
        this.commitFrequency = commitFrequency;
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);
        
        CopyStatementNode other = (CopyStatementNode)node;
        this.tableName = (TableName)getNodeFactory().copyNode(other.tableName,
                                                                getParserContext());
        this.subquery = (SubqueryNode)getNodeFactory().copyNode(other.subquery,
                                                                getParserContext());
        this.columnList = (ResultColumnList)getNodeFactory().copyNode(other.columnList,
                                                                      getParserContext());
        this.filename = other.filename;
        this.format = other.format;
        this.delimiter = other.delimiter;
        this.nullString = other.nullString;
        this.quote = other.quote;
        this.escape = other.escape;
        this.encoding = other.encoding;
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return This object as a String
     */

    public String toString() {
        return "mode: " + mode + "\n" +
            "tableName: " + tableName + "\n" +
            "filename: " + filename + "\n" +
            "format: " + format + "\n" +
            "delimiter: " + delimiter + "\n" +
            "nullString: " + nullString + "\n" +
            "quote: " + quote + "\n" +
            "escape: " + escape + "\n" +
            "encoding: " + encoding + "\n" +
            "commitFrequency: " + commitFrequency + "\n" +
            super.toString();
    }

    public String statementToString() {
        return "COPY";
    }

    /**
     * Prints the sub-nodes of this object.  See QueryTreeNode.java for
     * how tree printing is supposed to work.
     *
     * @param depth The depth of this node in the tree
     */

    public void printSubNodes(int depth) {
        super.printSubNodes(depth);

        if (subquery != null) {
            printLabel(depth, "subquery: ");
            subquery.treePrint(depth + 1);
        }
        if (columnList != null) {
            printLabel(depth, "columnList: ");
            columnList.treePrint(depth + 1);
        }
    }

    /**
     * Accept the visitor for all visitable children of this node.
     * 
     * @param v the visitor
     *
     * @exception StandardException on error
     */
    void acceptChildren(Visitor v) throws StandardException {
        super.acceptChildren(v);

        if (subquery != null) {
            subquery = (SubqueryNode)subquery.accept(v);
        }
        if (columnList != null) {
            columnList = (ResultColumnList)columnList.accept(v);
        }
    }

    /** Turn the source portion into a regular Select query. */
    public CursorNode asQuery() throws StandardException {
        NodeFactory nodeFactory = getNodeFactory();
        SQLParserContext parserContext = getParserContext();

        ResultSetNode resultSet;
        OrderByList orderBy = null;
        ValueNode offset = null, limit = null;
        if (subquery != null) {
            // Easy case: already specified as a subquery.
            resultSet = subquery.getResultSet();
            orderBy = subquery.getOrderByList();
            offset = subquery.getOffset();
            limit = subquery.getFetchFirst();
        }
        else {
            // Table case.
            FromList fromList = (FromList)nodeFactory.getNode(NodeTypes.FROM_LIST,
                                                              parserContext);
            FromTable fromTable = (FromTable)nodeFactory.getNode(NodeTypes.FROM_BASE_TABLE,
                                                                 tableName,
                                                                 null, null,
                                                                 parserContext);
            fromList.addFromTable(fromTable);
            ResultColumnList selectList = columnList;
            if (selectList == null) {
                selectList = (ResultColumnList)nodeFactory.getNode(NodeTypes.RESULT_COLUMN_LIST,
                                                                   parserContext);
                ResultColumn star = (ResultColumn)nodeFactory.getNode(NodeTypes.ALL_RESULT_COLUMN,
                                                                      Boolean.FALSE,
                                                                      parserContext);
                selectList.addResultColumn(star);
            }
            resultSet = (SelectNode)nodeFactory.getNode(NodeTypes.SELECT_NODE,
                                                        selectList,
                                                        null,
                                                        fromList,
                                                        null, null, null, null,
                                                        parserContext);
        }
        return (CursorNode)nodeFactory.getNode(NodeTypes.CURSOR_NODE,
                                               "SELECT",
                                               resultSet,
                                               null,
                                               orderBy, offset, limit,
                                               null, null,
                                               parserContext);
    }

}
