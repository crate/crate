'use strict';

angular.module('crateAdminApp', [])
  .config(function ($routeProvider) {
    $routeProvider
      .when('/', {
        templateUrl: 'views/main.html',
        controller: 'MainCtrl'
      })
      .when('/sqlconsole', {
        templateUrl: 'views/sqlconsole.html',
        controller: 'SqlconsoleCtrl'
      })
      .otherwise({
        redirectTo: '/'
      });
  });
