'use strict';

var crateAdminApp = angular.module('crateAdminApp', [
  'ngRoute',
  'sql',
  'stats',
  'common',
  'overview',
  'console',
  'tables',
  'cluster'
]);

crateAdminApp.config(['$routeProvider',
  function ($routeProvider) {
    $routeProvider
      .when('/', {
        templateUrl: 'views/overview.html',
        controller: 'OverviewController'
      })
      .when('/console', {
        templateUrl: 'views/console.html',
        controller: 'ConsoleController'
      })
      .when('/tables', {
        templateUrl: 'views/tables.html',
        controller: 'TablesController'
      })
      .when('/tables/:table_name', {
        templateUrl: 'views/tables.html',
        controller: 'TablesController'
      })
      .when('/cluster', {
        templateUrl: 'views/cluster.html',
        controller: 'ClusterController'
      })
      .when('/cluster/:node_name', {
        templateUrl: 'views/cluster.html',
        controller: 'ClusterController'
      })
      .otherwise({
        redirectTo: '/'
      });
  }]);

crateAdminApp.run(function(ClusterState) {});
