'use strict';

angular.module('common', ['stats'])
  .controller('StatusBarController', function ($scope, $log, $location, ClusterState) {
   $scope.$watch( function () { return ClusterState.data; }, function (data) {
     $scope.cluster_state = data.status;
     $scope.cluster_name = data.name;
     $scope.cluster_color_label = data.color_label;
     $scope.load1 = data.load[0] == '-.-' ? data.load[0] : data.load[0].toFixed(2);
     $scope.load5 = data.load[1] == '-.-' ? data.load[1] : data.load[1].toFixed(2);
     $scope.load15 = data.load[2] == '-.-' ? data.load[2] : data.load[2].toFixed(2);

   }, true);

   var prefix = $location.search().prefix || '';
   $scope.docs_url = prefix + "/_plugin/docs";

  })
  .controller('NavigationController', function ($scope, $location, ClusterState) {

    $scope.$watch( function () { return ClusterState.data; }, function (data) {
     $scope.cluster_color_label_bar = data.color_label;
     if (data.color_label == 'label-success' || data.color_label == 'label-default') {
       $scope.cluster_color_label_bar = '';
     }
    }, true);


    $scope.isActive = function (viewLocation) {
        return viewLocation === $location.path();
    };
  });
