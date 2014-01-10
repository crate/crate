package org.cratedb.module;

import org.cratedb.DataType;
import org.cratedb.action.TransportSQLReduceHandler;
import org.cratedb.action.collect.scope.ClusterNameExpression;
import org.cratedb.action.collect.scope.ScopedExpression;
import org.cratedb.action.groupby.aggregate.AggFunction;
import org.cratedb.action.groupby.aggregate.any.AnyAggFunction;
import org.cratedb.action.groupby.aggregate.avg.AvgAggFunction;
import org.cratedb.action.groupby.aggregate.count.CountColumnAggFunction;
import org.cratedb.action.groupby.aggregate.count.CountDistinctAggFunction;
import org.cratedb.action.groupby.aggregate.count.CountStarAggFunction;
import org.cratedb.action.groupby.aggregate.max.MaxAggFunction;
import org.cratedb.action.groupby.aggregate.min.MinAggFunction;
import org.cratedb.action.groupby.aggregate.sum.SumAggFunction;
import org.cratedb.action.sql.NodeExecutionContext;
import org.cratedb.action.sql.SQLAction;
import org.cratedb.action.sql.TransportSQLAction;
import org.cratedb.action.sql.analyzer.AnalyzerService;
import org.cratedb.action.sql.analyzer.ClusterUpdateCrateSettingsAction;
import org.cratedb.action.sql.analyzer.TransportClusterUpdateCrateSettingsAction;
import org.cratedb.information_schema.*;
import org.cratedb.service.InformationSchemaService;
import org.cratedb.service.SQLParseService;
import org.cratedb.sql.types.*;
import org.elasticsearch.action.GenericAction;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.assistedinject.FactoryProvider;
import org.elasticsearch.common.inject.multibindings.MapBinder;


public class SQLModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(SQLParseService.class).asEagerSingleton();
        bind(TransportSQLAction.class).asEagerSingleton();
        bind(InformationSchemaService.class).asEagerSingleton();
        bind(TransportClusterUpdateCrateSettingsAction.class).asEagerSingleton();
        bind(NodeExecutionContext.class).asEagerSingleton();
        bind(AnalyzerService.class).asEagerSingleton();
        bind(TransportSQLReduceHandler.class).asEagerSingleton();
        MapBinder<GenericAction, TransportAction> transportActionsBinder = MapBinder.newMapBinder(binder(), GenericAction.class,
                TransportAction.class);

        MapBinder<String, AggFunction> aggFunctionBinder =
            MapBinder.newMapBinder(binder(), String.class, AggFunction.class);
        aggFunctionBinder.addBinding(CountColumnAggFunction.NAME).to(CountColumnAggFunction.class).asEagerSingleton();
        aggFunctionBinder.addBinding(CountStarAggFunction.NAME).to(CountStarAggFunction.class).asEagerSingleton();
        aggFunctionBinder.addBinding(CountDistinctAggFunction.NAME).to(CountDistinctAggFunction.class).asEagerSingleton();
        aggFunctionBinder.addBinding(MinAggFunction.NAME).to(MinAggFunction.class).asEagerSingleton();
        aggFunctionBinder.addBinding(MaxAggFunction.NAME).to(MaxAggFunction.class).asEagerSingleton();
        aggFunctionBinder.addBinding(SumAggFunction.NAME).to(SumAggFunction.class).asEagerSingleton();
        aggFunctionBinder.addBinding(AvgAggFunction.NAME).to(AvgAggFunction.class).asEagerSingleton();
        aggFunctionBinder.addBinding(AnyAggFunction.NAME).to(AnyAggFunction.class).asEagerSingleton();

        transportActionsBinder.addBinding(SQLAction.INSTANCE).to(TransportSQLAction.class).asEagerSingleton();
        transportActionsBinder.addBinding(ClusterUpdateCrateSettingsAction.INSTANCE).to(TransportClusterUpdateCrateSettingsAction.class).asEagerSingleton();

        MapBinder<String, GenericAction> actionsBinder = MapBinder.newMapBinder(binder(), String.class, GenericAction.class);
        actionsBinder.addBinding(SQLAction.NAME).toInstance(SQLAction.INSTANCE);
        actionsBinder.addBinding(ClusterUpdateCrateSettingsAction.NAME).toInstance(ClusterUpdateCrateSettingsAction.INSTANCE);

        // Information schema tables
        MapBinder<String, InformationSchemaTable> informationSchemaTables = MapBinder
                .newMapBinder(binder(), String.class, InformationSchemaTable.class);
        informationSchemaTables.addBinding(TablesTable.NAME).to(TablesTable.class).asEagerSingleton();
        informationSchemaTables.addBinding(TableConstraintsTable.NAME).to(TableConstraintsTable.class)
                .asEagerSingleton();
        informationSchemaTables.addBinding(RoutinesTable.NAME).to(RoutinesTable.class).asEagerSingleton();
        informationSchemaTables.addBinding(ColumnsTable.NAME).to(ColumnsTable.class).asEagerSingleton();
        informationSchemaTables.addBinding(IndicesTable.NAME).to(IndicesTable.class).asEagerSingleton();

        // SQL Types
        MapBinder<DataType, SQLType> sqlTypeMapBinder = MapBinder.newMapBinder(binder(),
                DataType.class, SQLType.class);
        sqlTypeMapBinder.addBinding(DataType.BOOLEAN).to(BooleanSQLType.class).asEagerSingleton();
        sqlTypeMapBinder.addBinding(DataType.STRING).to(StringSQLType.class).asEagerSingleton();
        sqlTypeMapBinder.addBinding(DataType.BYTE).to(ByteSQLType.class).asEagerSingleton();
        sqlTypeMapBinder.addBinding(DataType.SHORT).to(ShortSQLType.class).asEagerSingleton();
        sqlTypeMapBinder.addBinding(DataType.INTEGER).to(IntegerSQLType.class).asEagerSingleton();
        sqlTypeMapBinder.addBinding(DataType.LONG).to(LongSQLType.class).asEagerSingleton();
        sqlTypeMapBinder.addBinding(DataType.FLOAT).to(FloatSQLType.class).asEagerSingleton();
        sqlTypeMapBinder.addBinding(DataType.DOUBLE).to(DoubleSQLType.class).asEagerSingleton();
        sqlTypeMapBinder.addBinding(DataType.TIMESTAMP).to(TimeStampSQLType.class).asEagerSingleton();
        sqlTypeMapBinder.addBinding(DataType.OBJECT).to(ObjectSQLType.class).asEagerSingleton();
        sqlTypeMapBinder.addBinding(DataType.IP).to(IpSQLType.class).asEagerSingleton();

        // get a factory for InformationSchemaTableExecutionContext
        bind(InformationSchemaTableExecutionContextFactory.class).toProvider(FactoryProvider
                .newFactory(InformationSchemaTableExecutionContextFactory.class,
                        InformationSchemaTableExecutionContext.class));

        // get a factory for SQLFieldMapper
        bind(SQLFieldMapperFactory.class).toProvider(FactoryProvider.newFactory
                (SQLFieldMapperFactory.class, SQLFieldMapper.class));

        // Cluster Level Expressions
        MapBinder<String, ScopedExpression> globalExpressions = MapBinder
                .newMapBinder(binder(), String.class, ScopedExpression.class);
        globalExpressions.addBinding(ClusterNameExpression.NAME).to(ClusterNameExpression.class).asEagerSingleton();
    }
}