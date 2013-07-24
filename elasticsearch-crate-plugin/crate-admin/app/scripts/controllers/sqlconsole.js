'use strict';

angular.module('crateAdminApp')
  .controller('SqlconsoleCtrl', function ($scope, $http, $location) {

    $scope.statement = "";
    $scope.result = {
        "rows": []
    }

    $scope.resultHeaders = [];
    $scope.error = {};
    $scope.error.hide = true;

    $scope.execute = function() {
        var prefix = $location.search().prefix || '';

        $http.post(prefix + "/_sql", {
            "stmt": $scope.statement
        }).success(function(data) {
            $scope.error.hide = true;

            $scope.resultHeaders = [];
            for (var i = 0; i < 1; i++) {
                for (var property in data.rows[i]) {
                    $scope.resultHeaders.push(property);
                }
                $scope.resultHeaders.sort();
            }

            $scope.result = data;
        }).error(function(data) {
            $scope.error.hide = false;
            $scope.error.message = data.error;
        });
    };
  });
