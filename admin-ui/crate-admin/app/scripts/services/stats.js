'use strict';

angular.module('stats', [])
  .factory('ClusterState', ['$http', '$timeout', '$location', '$log', '$rootScope', function ($http, $timeout, $location, $log, $rootScope) {
      var prefix = $location.search().prefix || '';
      var colorMap = {green: 'label-success',
                      yellow: 'label-warning',
                      red: 'label-important'};
      var data = {
        cluster_name: ''
      };

      var refresh = function() {
        $http({method: 'GET', url: prefix + '/_cluster/health'}).
          success(function(res_data) {
            $log.info("Received stats from cluster: " + res_data);
            $log.info("Received stats from cluster, name: " + res_data.cluster_name);
            data.name = res_data.cluster_name;
            data.status = res_data.status;
            data.color = colorMap[res_data.status];
            //$scope.nodes = data.number_of_nodes;
            //$scope.shards = data.active_primary_shards;
          }).
          error(function() {
          });
        $timeout(refresh, 5000);
      };

      refresh();
      return data;
  }]);