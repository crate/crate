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

/* The original from which this derives bore the following: */

/*

   Derby - Class org.apache.derby.impl.sql.compile.ValueNode

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.cratedb.sql.parser.parser;

import org.cratedb.sql.parser.StandardException;
import org.cratedb.sql.parser.types.DataTypeDescriptor;
import org.cratedb.sql.parser.types.TypeId;

/**
 * A ValueNode is an abstract class for all nodes that can represent data
 * values, that is, constants, columns, and expressions.
 *
 */

public abstract class ValueNode extends QueryTreeNode
{
    /**
     * The data type for this node.
     */
    private DataTypeDescriptor type;

    /*
    ** Constructor for untyped ValueNodes, for example, untyped NULLs
    ** and parameter nodes.
    **
    ** Binding will replace all untyped ValueNodes with typed ValueNodes
    ** when it figures out what their types should be.
    */
    public ValueNode() {
    }
        
    /**
     * Set this node's type from type components.
     */
    final void setType(TypeId typeId, boolean isNullable, int maximumWidth)
            throws StandardException {
        setType(new DataTypeDescriptor(typeId, isNullable, maximumWidth));
    }

    /**
     * Set this node's type from type components.
     */
    final void setType(TypeId typeId,
                       int precision, int scale,
                       boolean isNullable, int maximumWidth)
            throws StandardException {
        setType(new DataTypeDescriptor(typeId,
                                       precision, scale,
                                       isNullable, maximumWidth));     
    }

    /**
     * Initializer for numeric types.
     * 
     *
     * @param typeId The TypeID of this new node
     * @param precision The precision of this new node
     * @param scale The scale of this new node
     * @param isNullable The nullability of this new node
     * @param maximumWidth The maximum width of this new node
     *
     * @exception StandardException
     */

    public void init(Object typeId,
                     Object precision,
                     Object scale,
                     Object isNullable,
                     Object maximumWidth)
            throws StandardException {
        setType(new DataTypeDescriptor((TypeId)typeId,
                                       ((Integer)precision).intValue(),
                                       ((Integer)scale).intValue(),
                                       ((Boolean)isNullable).booleanValue(),
                                       ((Integer)maximumWidth).intValue()));
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        ValueNode other = (ValueNode)node;
        this.type = other.type;
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return This object as a String
     */

    public String toString() {
        return "type: " +
            ((type != null) ? type.toString() : "null" ) + "\n" +
            super.toString();
    }

    /**
     * Get the DataTypeDescriptor from this ValueNode.
     *
     * @return The DataTypeDescriptor from this ValueNode.  This
     *               may be null if the node isn't bound yet.
     */
    public DataTypeDescriptor getType() {
        return type;
    }
        
    /**
     * Set the nullability of this value.
     * @throws StandardException 
     */
    public void setNullability(boolean nullability) throws StandardException {
        setType(getType().getNullabilityType(nullability));
    }

    /**
     * Get the TypeId from this ValueNode.
     *
     * @return The TypeId from this ValueNode.  This
     *               may be null if the node isn't bound yet.
     */
    public TypeId getTypeId() throws StandardException {
        DataTypeDescriptor dtd = getType();
        if (dtd != null)
            return dtd.getTypeId();
        return null;
    }

    /**
     * Set the DataTypeDescriptor for this ValueNode.    This method is
     * overridden in ParameterNode.
     *
     * @param type The DataTypeDescriptor to set in this ValueNode
     */

    public void setType(DataTypeDescriptor type) throws StandardException {
        this.type = type;
    }

    /**
     * Get the source for this ValueNode.
     *
     * @return The source of this ValueNode, null if this node
     * is not sourced by a column.
     */

    public ResultColumn getSourceResultColumn() {
        return null;
    }

    /**
     * This returns the user-supplied schema name of the column.
     * At this class level, it simply returns null. But, the subclasses
     * of ValueNode will overwrite this method to return the
     * user-supplied schema name.
     * 
     * When the value node is in a result column of a select list,
     * the user can request metadata information. The result column
     * won't have a column descriptor, so we return some default
     * information through the expression. This lets expressions that
     * are simply columns return all of the info, and others use
     * this supertype's default values.
     *
     * @return the default schema name for an expression -- null
     */
    public String getSchemaName() throws StandardException {
        return null;
    }

    /**
     * This returns the user-supplied table name of the column.
     * At this class level, it simply returns null. But, the subclasses
     * of ValueNode will overwrite this method to return the
     * user-supplied table name.
     *
     * When the value node is in a result column of a select list,
     * the user can request metadata information. The result column
     * won't have a column descriptor, so we return some default
     * information through the expression. This lets expressions that
     * are simply columns return all of the info, and others use
     * this supertype's default values.
     *
     * @return the default table name for an expression -- null
     */
    public String getTableName() {
        return null;
    }

    /**
     * This is null so that the caller will substitute in the resultset generated
     * name as needed.
     *
     * @return the default column name for an expression -- null.
     */
    public String getColumnName() {
        return null;
    }

    /**
     * Return whether or not this expression tree represents a constant expression.
     *
     * @return Whether or not this expression tree represents a constant expression.
     */
    public boolean isConstantExpression() {
        return false;
    }

    /**
     * Return an Object representing the bind time value of this
     * expression tree.  If the expression tree does not evaluate to
     * a constant at bind time then we return null.
     * This is useful for bind time resolution of VTIs.
     * RESOLVE: What do we do for primitives?
     *
     * @return An Object representing the bind time value of this expression tree.
     *               (null if not a bind time constant.)
     *
     * @exception StandardException Thrown on error
     */
    Object getConstantValueAsObject() throws StandardException {
        return null;
    }

    /**
     * Does this represent a true constant.
     *
     * @return Whether or not this node represents a true constant.
     */
    public boolean isBooleanTrue() {
        return false;
    }

    /**
     * Does this represent a false constant.
     *
     * @return Whether or not this node represents a false constant.
     */
    public boolean isBooleanFalse() {
        return false;
    }

    /**
     * Returns true if this ValueNode is a relational operator. Relational
     * Operators are <, <=, =, >, >=, <> as well as IS NULL and IS NOT
     * NULL. This is the preferred way of figuring out if a ValueNode is
     * relational or not. 
     * @see RelationalOperator
     * @see BinaryRelationalOperatorNode
     * @see IsNullNode
     */
    public boolean isRelationalOperator() {
        return false;
    }

    /**
     * Returns true if this value node is a <em>equals</em> operator. 
     *
     * @see ValueNode#isRelationalOperator
     */
    public boolean isBinaryEqualsOperatorNode() {
        return false;
    }

    /**
     * Returns true if this value node is an operator created
     * for optimized performance of an IN list.
     *
     * Or more specifically, returns true if this value node is
     * an equals operator of the form "col = ?" that we generated
     * during preprocessing to allow index multi-probing.
     */
    public boolean isInListProbeNode() {
        return false;
    }

    /**
     * Returns TRUE if this is a parameter node. We do lots of special things
     * with Parameter Nodes.
     *
     */
    public boolean isParameterNode() {
        return false;
    }

    /**
     * Tests if this node is equivalent to the specified ValueNode. Two 
     * ValueNodes are considered equivalent if they will evaluate to the same
     * value during query execution. 
     * <p> 
     * This method provides basic expression matching facility for the derived 
     * class of ValueNode and it is used by the language layer to compare the 
     * node structural form of the two expressions for equivalence at bind 
     * phase.    
     *  <p>
     * Note that it is not comparing the actual row values at runtime to produce 
     * a result; hence, when comparing SQL NULLs, they are considered to be 
     * equivalent and not unknown.  
     *  <p>
     * One usage case of this method in this context is to compare the select 
     * column expression against the group by expression to check if they are 
     * equivalent.  e.g.:
     *  <p>
     * SELECT c1+c2 FROM t1 GROUP BY c1+c2   
     *  <p>
     * In general, node equivalence is determined by the derived class of 
     * ValueNode.    But they generally abide to the rules below:
     *  <ul>
     * <li>The two ValueNodes must be of the same node type to be considered 
     *   equivalent.    e.g.:    CastNode vs. CastNode - equivalent (if their args 
     *   also match), ColumnReference vs CastNode - not equivalent.
     *   
     * <li>If node P contains other ValueNode(s) and so on, those node(s) must 
     *   also be of the same node type to be considered equivalent.
     *   
     * <li>If node P takes a parameter list, then the number of arguments and its 
     *   arguments for the two nodes must also match to be considered 
     *   equivalent.    e.g.:    CAST(c1 as INTEGER) vs CAST(c1 as SMALLINT), they 
     *   are not equivalent.
     *   
     * <li>When comparing SQL NULLs in this context, they are considered to be 
     *   equivalent.
     * 
     * <li>If this does not apply or it is determined that the two nodes are not 
     *   equivalent then the derived class of this method should return false; 
     *   otherwise, return true.
     * </ul>     
     *   
     * @param other the node to compare this ValueNode against.
     * @return <code>true</code> if the two nodes are equivalent, 
     * <code>false</code> otherwise.
     * 
     * @throws StandardException 
     */
    protected abstract boolean isEquivalent(ValueNode other)
            throws StandardException;

    /**
     * Tests if this node is of the same type as the specified node as
     * reported by {@link QueryTreeNode#getNodeType()}.
     * 
     * @param other the node to compare this value node against. 
     * 
     * @return <code>true</code> if the two nodes are of the same type.  
     */
    protected final boolean isSameNodeType(ValueNode other) {
        if (other != null) {
            return other.getNodeType() == getNodeType();
        }
        return false;
    }

}
