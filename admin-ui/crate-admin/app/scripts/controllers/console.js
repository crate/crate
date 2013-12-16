'use strict';

angular.module('console', ['sql'])
  .controller('ConsoleController', function ($scope, $http, $location, SQLQuery, $log) {

    $scope.statement = "";
    $scope.result = {
      "rows": []
    }

    $('iframe').hide();

    $scope.resultHeaders = [];
    $scope.renderTable = false;
    $scope.error = {};
    $scope.error.hide = true;

    $scope.execute = function() {
      SQLQuery.execute($scope.statement).
        success(function(sqlQuery) {
          $scope.error.hide = true;
          $scope.renderTable = true;

          $scope.resultHeaders = [];
          for (var col in sqlQuery.cols) {
              $scope.resultHeaders.push(sqlQuery.cols[col]);
          }

          $scope.rows = sqlQuery.rows;
        }).
        error(function(sqlQuery) {
          $scope.error.hide = false;
          if (sqlQuery) {
            $scope.error.message = sqlQuery.error.message;
          } else {
            $scope.error.message = 'No Connection';
          }
        });
    };
  });
