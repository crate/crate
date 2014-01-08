'use strict';

var crateAdminApp = angular.module('crateAdminApp', [
  'ngRoute',
  'sql',
  'stats',
  'common',
  'overview',
  'console',
  'tables'
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
      .otherwise({
        redirectTo: '/'
      });
  }]);

crateAdminApp.run(function(ClusterState) {});
