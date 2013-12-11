'use strict';

angular.module('overview', ['stats'])
  .controller('OverviewController', [ '$scope', '$log', 'ClusterState', function ($scope, $log, ClusterState) {
    $scope.$watch( function () { return ClusterState.data; }, function (data) {
      $scope.cluster_state = data.status;
      $scope.cluster_color_label = data.color_label;

      $scope.replicated_data = '90%';
      $scope.available_data = '100%';
      $scope.records_total = 235000;
      $scope.records_underreplicated = 20000;
      $scope.records_unavailable = 0;
    }, true);

  }]);
