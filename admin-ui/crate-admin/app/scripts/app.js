'use strict';

var crateAdminApp = angular.module('crateAdminApp', [
  'ngRoute',
  'stats',
  'common',
  'overview',
  'console',
  'docs'
]);

crateAdminApp.config(['$routeProvider',
  function ($routeProvider) {
    $routeProvider
      .when('/', {
        templateUrl: 'views/empty_overview.html',
        controller: 'OverviewController'
      })
      .when('/console', {
        templateUrl: 'views/console.html',
        controller: 'ConsoleController'
      })
      .when('/docs', {
        templateUrl: 'views/docs.html',
        controller: 'DocsController'
      })
      .otherwise({
        redirectTo: '/'
      });
  }]);

crateAdminApp.run(function(ClusterState) {});
