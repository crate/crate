'use strict';

angular.module('common', ['stats'])
  .controller('StatusBarController', [ '$scope', '$log', 'ClusterState', function ($scope, $log, ClusterState) {
   $scope.$watch( function () { return ClusterState.data; }, function (data) {
     $scope.cluster_state = data.status;
     $scope.cluster_name = data.name;
     $scope.cluster_color = data.color;
     $scope.load1 = data.load[0].toFixed(2);
     $scope.load5 = data.load[1].toFixed(2);
     $scope.load15 = data.load[2].toFixed(2);
   }, true);

  }])
  .controller('NavigationController', [ '$scope', '$location', function ($scope, $location) {
    $scope.isActive = function (viewLocation) {
        return viewLocation === $location.path();
    };
  }]);
