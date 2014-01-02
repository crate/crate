package org.cratedb.sql.parser.parser;

/**
 *
 */
public interface TypedNodeFactory {

    public QueryTreeNode newNode();


    /**
     * Factories in here are derived from the NodeNames class and provide defaults for use by NodeType instances.
     */
    public static class Factories {

        // The names are in alphabetic order.

        public static final TypedNodeFactory AGGREGATE_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new AggregateNode();
            }
        };

        public static final TypedNodeFactory AGGREGATE_WINDOW_FUNCTION = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new AggregateWindowFunctionNode();
            }
        };

        public static final TypedNodeFactory ALL_RESULT_COLUMN = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new AllResultColumn();
            }
        };

        public static final TypedNodeFactory ALTER_SERVER_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new AlterServerNode();
            }
        };

        public static final TypedNodeFactory ALTER_TABLE_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new AlterTableNode();
            }
        };

        public static final TypedNodeFactory ANALYZER_ELEMENTS = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new AnalyzerElements();
            }
        };

        public static final TypedNodeFactory AT_DROP_INDEX_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new AlterDropIndexNode();
            }
        };

        public static final TypedNodeFactory AT_ADD_INDEX_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new AlterAddIndexNode();
            }
        };

        public static final TypedNodeFactory AT_RENAME_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new AlterTableRenameNode();
            }
        };

        public static final TypedNodeFactory AT_RENAME_COLUMN_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new AlterTableRenameColumnNode();
            }
        };

        public static final TypedNodeFactory AND_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new AndNode();
            }
        };

        public static final TypedNodeFactory BASE_COLUMN_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new BaseColumnNode();
            }
        };

        public static final TypedNodeFactory BETWEEN_OPERATOR_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new BetweenOperatorNode();
            }
        };

        public static final TypedNodeFactory BINARY_ARITHMETIC_OPERATOR_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new BinaryArithmeticOperatorNode();
            }
        };

        public static final TypedNodeFactory BINARY_BIT_OPERATOR_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new BinaryBitOperatorNode();
            }
        };

        public static final TypedNodeFactory BINARY_OPERATOR_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new BinaryOperatorNode();
            }
        };

        public static final TypedNodeFactory BINARY_RELATIONAL_OPERATOR_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new BinaryRelationalOperatorNode();
            }
        };

        public static final TypedNodeFactory LEFT_RIGHT_FUNC_OPERATOR_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new LeftRightFuncOperatorNode();
            }
        };

        public static final TypedNodeFactory ROW_CTOR_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new RowConstructorNode();
            }
        };

        public static final TypedNodeFactory BIT_CONSTANT_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new BitConstantNode();
            }
        };

        public static final TypedNodeFactory BOOLEAN_CONSTANT_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new BooleanConstantNode();
            }
        };

        public static final TypedNodeFactory CALL_STATEMENT_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new CallStatementNode();
            }
        };

        public static final TypedNodeFactory CAST_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new CastNode();
            }
        };

        public static final TypedNodeFactory CHAR_CONSTANT_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new CharConstantNode();
            }
        };

        public static final TypedNodeFactory CHAR_FILTER_LIST = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new CharFilterList();
            }
        };

        public static final TypedNodeFactory CLOSE_STATEMENT_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new CloseStatementNode();
            }
        };

        public static final TypedNodeFactory COALESCE_FUNCTION_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new CoalesceFunctionNode();
            }
        };

        public static final TypedNodeFactory COLUMN_DEFINITION_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new ColumnDefinitionNode();
            }
        };

        public static final TypedNodeFactory COLUMN_REFERENCE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new ColumnReference();
            }
        };

        public static final TypedNodeFactory CONCATENATION_OPERATOR_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new ConcatenationOperatorNode();
            }
        };

        public static final TypedNodeFactory CONDITIONAL_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new ConditionalNode();
            }
        };

        public static final TypedNodeFactory CONSTRAINT_DEFINITION_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new ConstraintDefinitionNode();
            }
        };

        public static final TypedNodeFactory COPY_STATEMENT_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new CopyStatementNode();
            }
        };

        public static final TypedNodeFactory CREATE_ALIAS_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new CreateAliasNode();
            }
        };

        public static final TypedNodeFactory CREATE_ANALYZER_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new CreateAnalyzerNode();
            }
        };

        public static final TypedNodeFactory CREATE_INDEX_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new CreateIndexNode();
            }
        };

        public static final TypedNodeFactory CREATE_ROLE_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new CreateRoleNode();
            }
        };

        public static final TypedNodeFactory CREATE_SCHEMA_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new CreateSchemaNode();
            }
        };

        public static final TypedNodeFactory CREATE_SEQUENCE_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new CreateSequenceNode();
            }
        };

        public static final TypedNodeFactory CREATE_TABLE_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new CreateTableNode();
            }
        };

        public static final TypedNodeFactory CREATE_TRIGGER_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new CreateTriggerNode();
            }
        };

        public static final TypedNodeFactory CREATE_VIEW_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new CreateViewNode();
            }
        };

        public static final TypedNodeFactory CURRENT_DATETIME_OPERATOR_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new CurrentDatetimeOperatorNode();
            }
        };

        public static final TypedNodeFactory CURRENT_OF_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new CurrentOfNode();
            }
        };

        public static final TypedNodeFactory CURRENT_ROW_LOCATION_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new CurrentRowLocationNode();
            }
        };

        public static final TypedNodeFactory CURSOR_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new CursorNode();
            }
        };

        public static final TypedNodeFactory OCTET_LENGTH_OPERATOR_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new OctetLengthOperatorNode();
            }
        };

        public static final TypedNodeFactory DEALLOCATE_STATEMENT_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new DeallocateStatementNode();
            }
        };

        public static final TypedNodeFactory DECLARE_STATEMENT_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new DeclareStatementNode();
            }
        };

        public static final TypedNodeFactory DEFAULT_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new DefaultNode();
            }
        };

        public static final TypedNodeFactory DELETE_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new DeleteNode();
            }
        };

        public static final TypedNodeFactory DISTINCT_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new DistinctNode();
            }
        };

//        public static final TypedNodeFactory DML_MOD_STATEMENT_NODE = new TypedNodeFactory() {
//            @Override
//            public QueryTreeNode newNode() {
//                return new DMLModStatementNode();
//            }
//        };

        public static final TypedNodeFactory DROP_ALIAS_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new DropAliasNode();
            }
        };

        public static final TypedNodeFactory DROP_INDEX_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new DropIndexNode();
            }
        };

        public static final TypedNodeFactory DROP_GROUP_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new DropGroupNode();
            }
        };

        public static final TypedNodeFactory DROP_ROLE_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new DropRoleNode();
            }
        };

        public static final TypedNodeFactory DROP_SCHEMA_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new DropSchemaNode();
            }
        };

        public static final TypedNodeFactory DROP_SEQUENCE_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new DropSequenceNode();
            }
        };

        public static final TypedNodeFactory DROP_TABLE_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new DropTableNode();
            }
        };

        public static final TypedNodeFactory DROP_TRIGGER_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new DropTriggerNode();
            }
        };

        public static final TypedNodeFactory DROP_VIEW_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new DropViewNode();
            }
        };

        public static final TypedNodeFactory EXECUTE_STATEMENT_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new ExecuteStatementNode();
            }
        };

        public static final TypedNodeFactory EXPLAIN_STATEMENT_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new ExplainStatementNode();
            }
        };

        public static final TypedNodeFactory EXPLICIT_COLLATE_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new ExplicitCollateNode();
            }
        };

        public static final TypedNodeFactory EXTRACT_OPERATOR_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new ExtractOperatorNode();
            }
        };

        public static final TypedNodeFactory FETCH_STATEMENT_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new FetchStatementNode();
            }
        };

        public static final TypedNodeFactory FK_CONSTRAINT_DEFINITION_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new FKConstraintDefinitionNode();
            }
        };

        public static final TypedNodeFactory FROM_BASE_TABLE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new FromBaseTable();
            }
        };

        public static final TypedNodeFactory FROM_LIST = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new FromList();
            }
        };

        public static final TypedNodeFactory FROM_SUBQUERY = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new FromSubquery();
            }
        };

        public static final TypedNodeFactory FROM_VTI = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new FromVTI();
            }
        };

        public static final TypedNodeFactory FULL_OUTER_JOIN_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new FullOuterJoinNode();
            }
        };

        public static final TypedNodeFactory GENERATION_CLAUSE_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new GenerationClauseNode();
            }
        };

        public static final TypedNodeFactory GENERIC_PROPERTIES = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new GenericProperties();
            }
        };

        public static final TypedNodeFactory GET_CURRENT_CONNECTION_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new GetCurrentConnectionNode();
            }
        };

        public static final TypedNodeFactory GRANT_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new GrantNode();
            }
        };

        public static final TypedNodeFactory GRANT_ROLE_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new GrantRoleNode();
            }
        };

        public static final TypedNodeFactory GROUP_BY_COLUMN = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new GroupByColumn();
            }
        };

        public static final TypedNodeFactory GROUP_BY_LIST = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new GroupByList();
            }
        };

//        public static final TypedNodeFactory GROUP_BY_NODE = new TypedNodeFactory() {
//            @Override
//            public QueryTreeNode newNode() {
//                return new GroupByNode();
//            }
//        };

        public static final TypedNodeFactory GROUP_CONCAT_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new GroupConcatNode();
            }
        };

        public static final TypedNodeFactory HALF_OUTER_JOIN_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new HalfOuterJoinNode();
            }
        };

//        public static final TypedNodeFactory HASH_TABLE_NODE = new TypedNodeFactory() {
//            @Override
//            public QueryTreeNode newNode() {
//                return new HashTableNode();
//            }
//        };

        public static final TypedNodeFactory INDEX_COLUMN = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new IndexColumn();
            }
        };

        public static final TypedNodeFactory INDEX_COLUMN_LIST = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new IndexColumnList();
            }
        };

        public static final TypedNodeFactory INDEX_CONSTRAINT = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new IndexConstraintDefinitionNode();
            }
        };

        public static final TypedNodeFactory INDEX_HINT_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new IndexHintNode();
            }
        };

        public static final TypedNodeFactory INDEX_HINT_LIST = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new IndexHintList();
            }
        };

//        public static final TypedNodeFactory INDEX_TO_BASE_ROW_NODE = new TypedNodeFactory() {
//            @Override
//            public QueryTreeNode newNode() {
//                return new IndexToBaseRowNode();
//            }
//        };

        public static final TypedNodeFactory INSERT_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new InsertNode();
            }
        };

        public static final TypedNodeFactory INTERSECT_OR_EXCEPT_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new IntersectOrExceptNode();
            }
        };

        public static final TypedNodeFactory IN_LIST_OPERATOR_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new InListOperatorNode();
            }
        };

        public static final TypedNodeFactory IS_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new IsNode();
            }
        };

        public static final TypedNodeFactory IS_NULL_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new IsNullNode();
            }
        };

        public static final TypedNodeFactory JAVA_TO_SQL_VALUE_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new JavaToSQLValueNode();
            }
        };

        public static final TypedNodeFactory JOIN_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new JoinNode();
            }
        };

        public static final TypedNodeFactory LENGTH_OPERATOR_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new LengthOperatorNode();
            }
        };

        public static final TypedNodeFactory LIKE_OPERATOR_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new LikeEscapeOperatorNode();
            }
        };

        public static final TypedNodeFactory LOCK_TABLE_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new LockTableNode();
            }
        };

        public static final TypedNodeFactory MATCH_FUNCTION_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new MatchFunctionNode();
            }
        };

//        public static final TypedNodeFactory MATERIALIZE_RESULT_SET_NODE = new TypedNodeFactory() {
//            @Override
//            public QueryTreeNode newNode() {
//                return new MaterializeResultSetNode();
//            }
//        };

        public static final TypedNodeFactory MODIFY_COLUMN_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new ModifyColumnNode();
            }
        };

        public static final TypedNodeFactory NAMED_NODE_WITH_OPTIONAL_PROPERTIES = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new NamedNodeWithOptionalProperties();
            }
        };

        public static final TypedNodeFactory NESTED_COLUMN_REFERENCE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new NestedColumnReference();
            }
        };

        public static final TypedNodeFactory NEW_INVOCATION_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new NewInvocationNode();
            }
        };

        public static final TypedNodeFactory NEXT_SEQUENCE_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new NextSequenceNode();
            }
        };

        public static final TypedNodeFactory CURRENT_SEQUENCE_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new CurrentSequenceNode();
            }
        };

        public static final TypedNodeFactory NON_STATIC_METHOD_CALL_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new NonStaticMethodCallNode();
            }
        };

        public static final TypedNodeFactory NOP_STATEMENT_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new NOPStatementNode();
            }
        };

//        public static final TypedNodeFactory NORMALIZE_RESULT_SET_NODE = new TypedNodeFactory() {
//            @Override
//            public QueryTreeNode newNode() {
//                return new NormalizeResultSetNode();
//            }
//        };

        public static final TypedNodeFactory NOT_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new NotNode();
            }
        };

        public static final TypedNodeFactory NUMERIC_CONSTANT_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new NumericConstantNode();
            }
        };

        public static final TypedNodeFactory OR_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new OrNode();
            }
        };

        public static final TypedNodeFactory ORDER_BY_COLUMN = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new OrderByColumn();
            }
        };

        public static final TypedNodeFactory ORDER_BY_LIST = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new OrderByList();
            }
        };

//        public static final TypedNodeFactory ORDER_BY_NODE = new TypedNodeFactory() {
//            @Override
//            public QueryTreeNode newNode() {
//                return new OrderByNode();
//            }
//        };

        public static final TypedNodeFactory PARAMETER_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new ParameterNode();
            }
        };

        public static final TypedNodeFactory PARTITION_BY_COLUMN = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new PartitionByColumn();
            }
        };

        public static final TypedNodeFactory PARTITION_BY_LIST = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new PartitionByList();
            }
        };

//        public static final TypedNodeFactory PREDICATE_LIST = new TypedNodeFactory() {
//            @Override
//            public QueryTreeNode newNode() {
//                return new PredicateList();
//            }
//        };
//
//        public static final TypedNodeFactory PREDICATE = new TypedNodeFactory() {
//            @Override
//            public QueryTreeNode newNode() {
//                return new Predicate();
//            }
//        };

        public static final TypedNodeFactory PREPARE_STATEMENT_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new PrepareStatementNode();
            }
        };

        public static final TypedNodeFactory PRIVILEGE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new PrivilegeNode();
            }
        };

//        public static final TypedNodeFactory PROJECT_RESTRICT_NODE = new TypedNodeFactory() {
//            @Override
//            public QueryTreeNode newNode() {
//                return new ProjectRestrictNode();
//            }
//        };

        public static final TypedNodeFactory RENAME_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new RenameNode();
            }
        };

        public static final TypedNodeFactory RESULT_COLUMN_LIST = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new ResultColumnList();
            }
        };

        public static final TypedNodeFactory RESULT_COLUMN = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new ResultColumn();
            }
        };

        public static final TypedNodeFactory REVOKE_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new RevokeNode();
            }
        };

        public static final TypedNodeFactory REVOKE_ROLE_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new RevokeRoleNode();
            }
        };

        public static final TypedNodeFactory ROW_COUNT_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new RowCountNode();
            }
        };

        public static final TypedNodeFactory ROW_NUMBER_FUNCTION = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new RowNumberFunctionNode();
            }
        };

        public static final TypedNodeFactory ROW_RESULT_SET_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new RowResultSetNode();
            }
        };

        public static final TypedNodeFactory ROWS_RESULT_SET_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new RowsResultSetNode();
            }
        };

        public static final TypedNodeFactory SAVEPOINT_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new SavepointNode();
            }
        };

//        public static final TypedNodeFactory SCROLL_INSENSITIVE_RESULT_SET_NODE = new TypedNodeFactory() {
//            @Override
//            public QueryTreeNode newNode() {
//                return new ScrollInsensitiveResultSetNode();
//            }
//        };

        public static final TypedNodeFactory SELECT_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new SelectNode();
            }
        };

        public static final TypedNodeFactory SET_CONFIGURATION_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new SetConfigurationNode();
            }
        };

        public static final TypedNodeFactory SET_ROLE_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new SetRoleNode();
            }
        };

        public static final TypedNodeFactory SET_SCHEMA_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new SetSchemaNode();
            }
        };

        public static final TypedNodeFactory SET_TRANSACTION_ACCESS_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new SetTransactionAccessNode();
            }
        };

        public static final TypedNodeFactory SET_TRANSACTION_ISOLATION_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new SetTransactionIsolationNode();
            }
        };

        public static final TypedNodeFactory SIMPLE_CASE_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new SimpleCaseNode();
            }
        };

        public static final TypedNodeFactory SIMPLE_STRING_OPERATOR_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new SimpleStringOperatorNode();
            }
        };

        public static final TypedNodeFactory SPECIAL_FUNCTION_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new SpecialFunctionNode();
            }
        };

        public static final TypedNodeFactory SQL_BOOLEAN_CONSTANT_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new SQLBooleanConstantNode();
            }
        };

        public static final TypedNodeFactory SQL_TO_JAVA_VALUE_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new SQLToJavaValueNode();
            }
        };

        public static final TypedNodeFactory STATIC_CLASS_FIELD_REFERENCE_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new StaticClassFieldReferenceNode();
            }
        };

        public static final TypedNodeFactory STATIC_METHOD_CALL_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new StaticMethodCallNode();
            }
        };

        public static final TypedNodeFactory SUBQUERY_LIST = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new SubqueryList();
            }
        };

        public static final TypedNodeFactory SUBQUERY_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new SubqueryNode();
            }
        };

        public static final TypedNodeFactory SYSTEM_COLUMN_REFERENCE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new SystemColumnReference();
            }
        };

        public static final TypedNodeFactory TABLE_ELEMENT_LIST = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new TableElementList();
            }
        };

        public static final TypedNodeFactory TABLE_ELEMENT_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new TableElementNode();
            }
        };

        public static final TypedNodeFactory TABLE_NAME = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new TableName();
            }
        };

        public static final TypedNodeFactory TABLE_PRIVILEGES = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new TablePrivilegesNode();
            }
        };

        public static final TypedNodeFactory TERNARY_OPERATOR_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new TernaryOperatorNode();
            }
        };

        public static final TypedNodeFactory TEST_CONSTRAINT_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new TestConstraintNode();
            }
        };

        public static final TypedNodeFactory TIMESTAMP_OPERATOR_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new TimestampOperatorNode();
            }
        };

        public static final TypedNodeFactory TOKEN_FILTER_LIST = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new TokenFilterList();
            }
        };

        public static final TypedNodeFactory TRANSACTION_CONTROL_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new TransactionControlNode();
            }
        };

        public static final TypedNodeFactory TRIM_OPERATOR_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new TrimOperatorNode();
            }
        };

        public static final TypedNodeFactory UNARY_ARITHMETIC_OPERATOR_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new UnaryArithmeticOperatorNode();
            }
        };

        public static final TypedNodeFactory UNARY_BIT_OPERATOR_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new UnaryBitOperatorNode();
            }
        };

        public static final TypedNodeFactory UNARY_DATE_TIMESTAMP_OPERATOR_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new UnaryDateTimestampOperatorNode();
            }
        };

        public static final TypedNodeFactory UNARY_OPERATOR_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new UnaryOperatorNode();
            }
        };

        public static final TypedNodeFactory UNION_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new UnionNode();
            }
        };

        public static final TypedNodeFactory UNTYPED_NULL_CONSTANT_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new UntypedNullConstantNode();
            }
        };

        public static final TypedNodeFactory UPDATE_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new UpdateNode();
            }
        };

        public static final TypedNodeFactory USERTYPE_CONSTANT_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new UserTypeConstantNode();
            }
        };

        public static final TypedNodeFactory VALUE_NODE_LIST = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new ValueNodeList();
            }
        };

        public static final TypedNodeFactory VARBIT_CONSTANT_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new VarbitConstantNode();
            }
        };

        public static final TypedNodeFactory VIRTUAL_COLUMN_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new VirtualColumnNode();
            }
        };

        public static final TypedNodeFactory WINDOW_DEFINITION = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new WindowDefinitionNode();
            }
        };

        public static final TypedNodeFactory WINDOW_REFERENCE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new WindowReferenceNode();
            }
        };

//        public static final TypedNodeFactory WINDOW_RESULTSET_NODE = new TypedNodeFactory() {
//            @Override
//            public QueryTreeNode newNode() {
//                return new WindowResultSetNode();
//            }
//        };

        public static final TypedNodeFactory XML_BINARY_OPERATOR_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new XMLBinaryOperatorNode();
            }
        };

        public static final TypedNodeFactory XML_CONSTANT_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new XMLConstantNode();
            }
        };

        public static final TypedNodeFactory XML_UNARY_OPERATOR_NODE = new TypedNodeFactory() {
            @Override
            public QueryTreeNode newNode() {
                return new XMLUnaryOperatorNode();
            }
        };

    }

}
