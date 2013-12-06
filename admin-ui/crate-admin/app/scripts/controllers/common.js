'use strict';

angular.module('common', ['stats'])
  .controller('StatusBarController', [ '$scope', '$log', 'ClusterState', function ($scope, $log, ClusterState) {
    $scope.cluster_state = ClusterState.status;
    $scope.cluster_name = ClusterState.name;
    $scope.cluster_color = ClusterState.color;
  }])
  .controller('NavigationController', [ '$scope', '$location', function ($scope, $location) {
    $scope.isActive = function (viewLocation) {
        return viewLocation === $location.path();
    };
  }]);
