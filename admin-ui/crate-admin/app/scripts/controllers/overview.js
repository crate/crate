'use strict';

angular.module('overview', ['stats'])
  .controller('OverviewController', [ '$scope', '$log', 'ClusterState', function ($scope, $log, ClusterState) {
    $log.info("Cluster name: " + ClusterState.name);
    $scope.cluster_state = ClusterState.status;
    $scope.cluster_color = ClusterState.color;

    $scope.replicated_data = '90%';
    $scope.available_data = '100%';
    $scope.records_total = 235000;
    $scope.records_underreplicated = 20000;
    $scope.records_unavailable = 0;

  }]);
