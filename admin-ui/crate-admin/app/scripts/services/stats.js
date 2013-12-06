'use strict';

angular.module('stats', [])
  .factory('ClusterState', ['$http', '$timeout', '$location', '$log', '$rootScope', function ($http, $timeout, $location, $log, $rootScope) {
      var prefix = $location.search().prefix || '';
      var colorMap = {green: 'label-success',
                      yellow: 'label-warning',
                      red: 'label-important'};
      var data = {
        name: '',
        status: '',
        color: '',
        load: [0.0, 0.0, 0.0]
      };

      var refreshHealth = function() {
        $http({method: 'GET', url: prefix + '/_cluster/health'}).
          success(function(res_data) {
            data.name = res_data.cluster_name;
            data.status = res_data.status;
            data.color = colorMap[res_data.status];
            //$scope.nodes = data.number_of_nodes;
            //$scope.shards = data.active_primary_shards;
          }).
          error(function() {
          });
        $timeout(refreshHealth, 5000);
      };

      function clusterLoad(nodes) {
        var nodes_count = 0;
        var load = [0.0, 0.0, 0.0];
        for (var node in nodes) {
          nodes_count++;
          for (var i=0; i<3; i++) {
            load[i] = load[i]+nodes[node].os.load_average[i];
          }
        }
        for (var i; i<3; i++) {
          load[i] = load[i]/nodes_count;
        }
        return load;
      }

      var refreshState = function() {
        $http({method: 'GET', url: prefix + '/_nodes/stats?all=true'}).
          success(function(res_data) {
            data.load = clusterLoad(res_data.nodes);
          }).
          error(function() {
          });
        $timeout(refreshState, 5000);
      };

      refreshHealth();
      refreshState();

      return {
        data: data
      };
  }])
  ;