'use strict';

angular.module('crateAdminApp')
  .controller('SqlconsoleCtrl', function ($scope, $http, $location) {

    $scope.statement = "";
    $scope.result = {
        "rows": []
    }

    $('iframe').hide();

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
            for (var col in data.cols) {
                $scope.resultHeaders.push(data.cols[col]);
            }

            $scope.result = data;
        }).error(function(data) {
            $scope.error.hide = false;
            $scope.error.message = data.error;
        });
    };
  });
