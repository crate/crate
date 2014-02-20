/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

/* The original from which this derives bore the following: */

package org.cratedb.sql.parser.parser;

import org.cratedb.sql.parser.StandardException;
import org.cratedb.sql.parser.types.DataTypeDescriptor;

import java.sql.Types;
import java.util.Map;

import java.io.IOException;
import java.io.Writer;

/**
 * QueryTreeNode is the root class for all query tree nodes. All
 * query tree nodes inherit from QueryTreeNode.
 *
 */

public abstract class QueryTreeNode implements Visitable
{
    public static final int AUTOINCREMENT_START_INDEX = 0;
    public static final int AUTOINCREMENT_INC_INDEX     = 1;
    public static final int AUTOINCREMENT_IS_AUTOINCREMENT_INDEX     = 2;
    //Parser uses this static field to make a note if the autoincrement column 
    //is participating in create or alter table.
    public static final int AUTOINCREMENT_CREATE_MODIFY  = 3;

    private int beginOffset = -1; // offset into SQL input of the substring
    private int endOffset = -1;     // which this query node encodes.

    private NodeType nodeType;
    private SQLParserContext pc;
    private Object userData;

    /**
     * Set the parser context for this node.
     * 
     * @param pc The SQLParserContext
     */
    public void setParserContext(SQLParserContext pc) {
        this.pc = pc;
    }

    /**
     * Get the current parser context.
     *
     * @return The current SQLParserContext.
     */
    public SQLParserContext getParserContext() {
        return pc;
    }

    /**
     * Set the user data associated with this node.
     */
    public void setUserData(Object userData) {
        this.userData = userData;
    }

    /**
     * Get the user data associated with this node.
     */
    public Object getUserData() {
        return userData;
    }

    /**
     * Gets the NodeFactory for this database.
     *
     * @return the node factory for this parser.
     *
     */
    public NodeFactory getNodeFactory() {
        return pc.getNodeFactory();
    }

    /**
     * Fill this node with a deep copy of the given node.
     * Specific node classes must override to deep copy their data.
     */
    public void copyFrom(QueryTreeNode other) throws StandardException {
        this.userData = getNodeFactory().copyUserData(this, other.userData);
    }

    /**
     * Gets the beginning offset of the SQL substring which this
     * query node represents.
     *
     * @return The beginning offset of the SQL substring. -1 means unknown.
     *
     */
    public int getBeginOffset() { 
        return beginOffset; 
    }

    /**
     * Sets the beginning offset of the SQL substring which this
     * query node represents.
     *
     * @param beginOffset The beginning offset of the SQL substring.
     *
     */
    public void setBeginOffset(int beginOffset) {
        this.beginOffset = beginOffset;
    }

    /**
     * Gets the ending offset of the SQL substring which this
     * query node represents.
     *
     * @return The ending offset of the SQL substring. -1 means unknown.
     *
     */
    public int getEndOffset() { 
        return endOffset; 
    }

    /**
     * Sets the ending offset of the SQL substring which this
     * query node represents.
     *
     * @param endOffset The ending offset of the SQL substring.
     *
     */
    public void setEndOffset(int endOffset) {
        this.endOffset = endOffset;
    }

    /**
     * Return header information for debug printing of this query
     * tree node.
     *
     * @return Header information for debug printing of this query
     *               tree node.
     */

    protected String nodeHeader() {
        return "\n" + this.getClass().getName() + '@' +
            Integer.toHexString(hashCode()) + "\n";
    }

    /**
     * Format a node that has been converted to a String for printing
     * as part of a tree.    This method indents the String to the given
     * depth by inserting tabs at the beginning of the string, and also
     * after every newline.
     *
     * @param nodeString The node formatted as a String
     * @param depth The depth to indent the given node
     *
     * @return The node String reformatted with tab indentation
     */

    public static String formatNodeString(String nodeString, int depth) {
        StringBuffer nodeStringBuffer = new StringBuffer(nodeString);
        int pos;
        char c;
        char[] indent = new char[depth];

        /*
        ** Form an array of tab characters for indentation.
        */
        while (depth > 0) {
            indent[depth - 1] = '\t';
            depth--;
        }

        /* Indent the beginning of the string */
        nodeStringBuffer.insert(0, indent);

        /*
        ** Look for newline characters, except for the last character.
        ** We don't want to indent after the last newline.
        */
        for (pos = 0; pos < nodeStringBuffer.length() - 1; pos++) {
            c = nodeStringBuffer.charAt(pos);
            if (c == '\n') {
                /* Indent again after each newline */
                nodeStringBuffer.insert(pos + 1, indent);
            }
        }

        return nodeStringBuffer.toString();
    }

    /**
     * Print this tree for debugging purposes.  This recurses through
     * all the sub-nodes and prints them indented by their depth in
     * the tree.
     */

    public void treePrint() {
        debugPrint(nodeHeader());
        String thisStr = formatNodeString(this.toString(), 0);

        if (containsInfo(thisStr)) {
            debugPrint(thisStr);
        }

        printSubNodes(0);
        debugFlush();
    }

    /**
     * Print this tree to the given stream.
     */
    public void treePrint(Writer writer) {
        Writer oldWriter = getDebugOutput();
        try {
            setDebugOutput(writer);
            treePrint();
        }
        finally {
            setDebugOutput(oldWriter);
        }
    }

    /**
     * Print call stack for debug purposes
     */

    public void stackPrint() {
        debugPrint("Stacktrace:\n");
        Exception e = new Exception("dummy");
        StackTraceElement[] st= e.getStackTrace();
        for (int i=0; i<st.length; i++) {
            debugPrint(st[i] + "\n");
        }

        debugFlush();
    }

    /**
     * Print this tree for debugging purposes.  This recurses through
     * all the sub-nodes and prints them indented by their depth in
     * the tree, starting with the given indentation.
     *
     * @param depth The depth of this node in the tree, thus,
     * the amount to indent it when printing it.
     */

    public void treePrint(int depth) {
        Map printed = getParserContext().getPrintedObjectsMap();

        if (printed.containsKey(this)) {
            debugPrint(formatNodeString(nodeHeader(), depth));
            debugPrint(formatNodeString("***truncated***\n", depth));
        } 
        else {
            printed.put(this, null);
            debugPrint(formatNodeString(nodeHeader(), depth));
            String thisStr = formatNodeString(this.toString(), depth);

            if (containsInfo(thisStr)) {
                debugPrint(thisStr);
            }

            if (thisStr.charAt(thisStr.length()-1) != '\n') {
                debugPrint("\n");
            }

            printSubNodes(depth);
        }
    }

    private static boolean containsInfo(String str) {
        for (int i = 0; i < str.length(); i++) {
            if (str.charAt(i) != '\t' && str.charAt(i) != '\n') {
                return true;
            }
        }
        return false;
    }

    private static Writer debugOutput = null;

    public static Writer getDebugOutput() {
        return debugOutput;
    }
    public static void setDebugOutput(Writer writer) {
        debugOutput = writer;
    }

    /**
     * Print a String for debugging
     *
     * @param outputString The String to print
     */

    public static void debugPrint(String outputString) {
        if (debugOutput == null)
            System.out.print(outputString);
        else {
            try {
                debugOutput.write(outputString);
            }
            catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    /**
     * Flush the debug stream out
     */
    protected static void debugFlush() {
        if (debugOutput != null) {
            try {
                debugOutput.flush();
            }
            catch (IOException ex) {
                throw new RuntimeException(ex);              
            }
        }
    }

    /**
     * Print the sub-nodes of this node.
     *
     * Each sub-class of QueryTreeNode is expected to provide its own
     * printSubNodes() method.  In each case, it calls super.printSubNodes(),
     * passing along its depth, to get the sub-nodes of the super-class.
     * Then it prints its own sub-nodes by calling treePrint() on each
     * of its members that is a type of QueryTreeNode.  In each case where
     * it calls treePrint(), it should pass "depth + 1" to indicate that
     * the sub-node should be indented one more level when printing.
     * Also, it should call printLabel() to print the name of each sub-node
     * before calling treePrint() on the sub-node, so that the reader of
     * the printed tree can tell what the sub-node is.
     *
     * @param depth The depth to indent the sub-nodes
     */

    public void printSubNodes(int depth) {
        if (userData != null) {
            printLabel(depth, "userData: ");
            // TODO: Consiser an interface to allow for special method.
            debugPrint(userData.toString() + "\n");
        }
    }

    /**
     * Format this node as a string
     *
     * Each sub-class of QueryTreeNode should implement its own toString()
     * method.  In each case, toString() should format the class members
     * that are not sub-types of QueryTreeNode (printSubNodes() takes care
     * of following the INFOS to sub-nodes, and toString() takes care
     * of all members that are not sub-nodes).  Newlines should be used
     * liberally - one good way to do this is to have a newline at the
     * end of each formatted member.    It's also a good idea to put the
     * name of each member in front of the formatted value.  For example,
     * the code might look like:
     *
     * "memberName: " + memberName + "\n" + ...
     *
     * List members containing subclasses of QueryTreeNode should subclass
     * QueryTreeNodeList. Such subclasses form a special case: These classes
     * should not implement printSubNodes, since there is generic handling in
     * QueryTreeNodeList.    They should only implement toString if they
     * contain additional members.
     *
     * @return This node formatted as a String
     */

    public String toString() {
        return "";
    }

    /**
     * Print the given label at the given indentation depth.
     *
     * @param depth The depth of indentation to use when printing
     *                          the label
     * @param label The String to print
     */

    public void printLabel(int depth, String label) {
        debugPrint(formatNodeString(label, depth));
    }

    /**
     * Set the node type for this node.
     *
     * @param nodeType The node type.
     */
    public void setNodeType(NodeType nodeType) {
        this.nodeType = nodeType;
    }

    public NodeType getNodeType() {
        return nodeType;
    }

    /**
     * For final nodes, return whether or not
     * the node represents the specified nodeType.
     *
     * @param nodeType The nodeType of interest.
     *
     * @return Whether or not
     * the node represents the specified nodeType.
     */
    protected boolean isInstanceOf(NodeType nodeType) {
        return (this.nodeType == nodeType);
    }

    /**
     * Accept a visitor, and call {@code v.visit()} on child nodes as
     * necessary. Sub-classes should not override this method, but instead
     * override the {@link #acceptChildren(Visitor)} method.
     * 
     * @param v the visitor
     *
     * @exception StandardException on error
     */
    public final Visitable accept(Visitor v) throws StandardException {
        final boolean childrenFirst = v.visitChildrenFirst(this);
        final boolean skipChildren = v.skipChildren(this);

        if (childrenFirst && !skipChildren && !v.stopTraversal()) {
            acceptChildren(v);
        }

        final Visitable ret = v.stopTraversal() ? this : v.visit(this);

        if (!childrenFirst && !skipChildren && !v.stopTraversal()) {
            acceptChildren(v);
        }

        return ret;
    }

    /**
     * Accept a visitor on all child nodes. All sub-classes that add fields
     * that should be visited, should override this method and call
     * {@code accept(v)} on all visitable fields, as well as
     * {@code super.acceptChildren(v)} to make sure all visitable fields
     * defined by the super-class are accepted too.
     *
     * @param v the visitor
     * @throws StandardException on errors raised by the visitor
     */
    void acceptChildren(Visitor v) throws StandardException {
        // no children
    }

    /**
     * Return the type of statement, something from
     * StatementType.
     *
     * @return the type of statement
     */
    protected int getStatementType() {
        return StatementType.UNKNOWN;
    }

    /**
     * Get a ConstantNode to represent a typed null value. 
     *
     * @param type Type of the null node.
     *
     * @return A ConstantNode with the specified type, and a value of null
     *
     * @exception StandardException Thrown on error
     */
    public ConstantNode getNullNode(DataTypeDescriptor type) throws StandardException {
        NodeType constantNodeType;
        switch (type.getTypeId().getJDBCTypeId()) {
        case Types.VARCHAR:
            constantNodeType = NodeType.VARCHAR_CONSTANT_NODE;
            break;

        case Types.CHAR:
            constantNodeType = NodeType.CHAR_CONSTANT_NODE;
            break;

        case Types.TINYINT:
            constantNodeType = NodeType.TINYINT_CONSTANT_NODE;
            break;

        case Types.SMALLINT:
            constantNodeType = NodeType.SMALLINT_CONSTANT_NODE;
            break;

        case Types.INTEGER:
            constantNodeType = NodeType.INT_CONSTANT_NODE;
            break;

        case Types.BIGINT:
            constantNodeType = NodeType.LONGINT_CONSTANT_NODE;
            break;

        case Types.REAL:
            constantNodeType = NodeType.FLOAT_CONSTANT_NODE;
            break;

        case Types.DOUBLE:
            constantNodeType = NodeType.DOUBLE_CONSTANT_NODE;
            break;

        case Types.NUMERIC:
        case Types.DECIMAL:
            constantNodeType = NodeType.DECIMAL_CONSTANT_NODE;
            break;

        case Types.DATE:
        case Types.TIME:
        case Types.TIMESTAMP:
            constantNodeType = NodeType.USERTYPE_CONSTANT_NODE;
            break;

        case Types.BINARY:
            constantNodeType = NodeType.BIT_CONSTANT_NODE;
            break;

        case Types.VARBINARY:
            constantNodeType = NodeType.VARBIT_CONSTANT_NODE;
            break;

        case Types.LONGVARCHAR:
            constantNodeType = NodeType.LONGVARCHAR_CONSTANT_NODE;
            break;

        case Types.CLOB:
            constantNodeType = NodeType.CLOB_CONSTANT_NODE;
            break;

        case Types.LONGVARBINARY:
            constantNodeType = NodeType.LONGVARBIT_CONSTANT_NODE;
            break;

        case Types.BLOB:
            constantNodeType = NodeType.BLOB_CONSTANT_NODE;
            break;

        case Types.SQLXML:
            constantNodeType = NodeType.XML_CONSTANT_NODE;
            break;
                        
        case Types.BOOLEAN:
            constantNodeType = NodeType.BOOLEAN_CONSTANT_NODE;
            break;

        default:
            if (type.getTypeId().userType()) {
                constantNodeType = NodeType.USERTYPE_CONSTANT_NODE;
            }
            else {
                assert false : 
                "Unknown type " + type.getTypeId().getSQLTypeName() + " in getNullNode";
                return null;
            }
        }
                
        ConstantNode constantNode = (ConstantNode)getNodeFactory().getNode(constantNodeType,
                                                                           type.getTypeId(),
                                                                           pc);

        constantNode.setType(type.getNullabilityType(true));

        return constantNode;
    }

    /**
     * Translate a Default node into a default value, given a type descriptor.
     *
     * @param typeDescriptor A description of the required data type.
     *
     * @exception StandardException Thrown on error
     */
    public Object convertDefaultNode(DataTypeDescriptor typeDescriptor)
            throws StandardException {
        /*
        ** Override in cases where node type
        ** can be converted to default value.
        */
        return null;
    }

    /* Initializable methods */

    /**
     * Initialize a query tree node.
     *
     * @exception StandardException Thrown on error
     */
    public void init(Object arg1) throws StandardException {
        assert false : "Single-argument init() not implemented for " + getClass().getName();
    }


    /**
     * Initialize a query tree node.
     *
     * @exception StandardException Thrown on error
     */
    public void init(Object arg1,
                     Object arg2) 
            throws StandardException {
        assert false : "Two-argument init() not implemented for " + getClass().getName();
    }

    /**
     * Initialize a query tree node.
     *
     * @exception StandardException Thrown on error
     */
    public void init(Object arg1,
                     Object arg2,
                     Object arg3) 
            throws StandardException {
        assert false : "Three-argument init() not implemented for " + getClass().getName();
    }

    /**
     * Initialize a query tree node.
     *
     * @exception StandardException Thrown on error
     */
    public void init(Object arg1,
                     Object arg2,
                     Object arg3,
                     Object arg4) 
            throws StandardException {
        assert false : "Four-argument init() not implemented for " + getClass().getName();
    }

    /**
     * Initialize a query tree node.
     *
     * @exception StandardException Thrown on error
     */
    public void init(Object arg1,
                     Object arg2,
                     Object arg3,
                     Object arg4,
                     Object arg5) 
            throws StandardException {
        assert false : "Five-argument init() not implemented for " + getClass().getName();
    }

    /**
     * Initialize a query tree node.
     *
     * @exception StandardException Thrown on error
     */
    public void init(Object arg1,
                     Object arg2,
                     Object arg3,
                     Object arg4,
                     Object arg5,
                     Object arg6) 
            throws StandardException {
        assert false : "Six-argument init() not implemented for " + getClass().getName();
    }

    /**
     * Initialize a query tree node.
     *
     * @exception StandardException Thrown on error
     */
    public void init(Object arg1,
                     Object arg2,
                     Object arg3,
                     Object arg4,
                     Object arg5,
                     Object arg6,
                     Object arg7) 
            throws StandardException {
        assert false : "Seven-argument init() not implemented for " + getClass().getName();
    }

    /**
     * Initialize a query tree node.
     *
     * @exception StandardException Thrown on error
     */
    public void init(Object arg1,
                     Object arg2,
                     Object arg3,
                     Object arg4,
                     Object arg5,
                     Object arg6,
                     Object arg7,
                     Object arg8) 
            throws StandardException {
        assert false : "Eight-argument init() not implemented for " + getClass().getName();
    }

    /**
     * Initialize a query tree node.
     *
     * @exception StandardException Thrown on error
     */
    public void init(Object arg1,
                     Object arg2,
                     Object arg3,
                     Object arg4,
                     Object arg5,
                     Object arg6,
                     Object arg7,
                     Object arg8,
                     Object arg9) 
            throws StandardException {
        assert false : "Nine-argument init() not implemented for " + getClass().getName();
    }

    /**
     * Initialize a query tree node.
     *
     * @exception StandardException Thrown on error
     */
    public void init(Object arg1,
                     Object arg2,
                     Object arg3,
                     Object arg4,
                     Object arg5,
                     Object arg6,
                     Object arg7,
                     Object arg8,
                     Object arg9,
                     Object arg10) 
            throws StandardException {
        assert false : "Ten-argument init() not implemented for " + getClass().getName();
    }

    /**
     * Initialize a query tree node.
     *
     * @exception StandardException Thrown on error
     */
    public void init(Object arg1,
                     Object arg2,
                     Object arg3,
                     Object arg4,
                     Object arg5,
                     Object arg6,
                     Object arg7,
                     Object arg8,
                     Object arg9,
                     Object arg10,
                     Object arg11) 
            throws StandardException {
        assert false : "Eleven-argument init() not implemented for " + getClass().getName();
    }

    /**
     * Initialize a query tree node.
     *
     * @exception StandardException Thrown on error
     */
    public void init(Object arg1,
                     Object arg2,
                     Object arg3,
                     Object arg4,
                     Object arg5,
                     Object arg6,
                     Object arg7,
                     Object arg8,
                     Object arg9,
                     Object arg10,
                     Object arg11,
                     Object arg12) 
            throws StandardException {
        assert false : "Twelve-argument init() not implemented for " + getClass().getName();
    }

    /**
     * Initialize a query tree node.
     *
     * @exception StandardException Thrown on error
     */
    public void init(Object arg1,
                     Object arg2,
                     Object arg3,
                     Object arg4,
                     Object arg5,
                     Object arg6,
                     Object arg7,
                     Object arg8,
                     Object arg9,
                     Object arg10,
                     Object arg11,
                     Object arg12,
                     Object arg13) 
            throws StandardException {
        assert false : "Thirteen-argument init() not implemented for " + getClass().getName();
    }

    /**
     * Initialize a query tree node.
     *
     * @exception StandardException Thrown on error
     */
    public void init(Object arg1,
                     Object arg2,
                     Object arg3,
                     Object arg4,
                     Object arg5,
                     Object arg6,
                     Object arg7,
                     Object arg8,
                     Object arg9,
                     Object arg10,
                     Object arg11,
                     Object arg12,
                     Object arg13,
                     Object arg14) 
            throws StandardException {
        assert false : "Fourteen-argument init() not implemented for " + getClass().getName();
    }

    public TableName makeTableName (String schemaName, String flatName)
            throws StandardException {
        return makeTableName(getNodeFactory(), getParserContext(), schemaName, flatName);
    }

    public static TableName makeTableName(NodeFactory nodeFactory,
                                          SQLParserContext parserContext,
                                          String schemaName,
                                          String flatName)
            throws StandardException {
        return (TableName)nodeFactory.getNode(NodeType.TABLE_NAME,
                                              schemaName,
                                              flatName,
                                              parserContext);
    }

}
