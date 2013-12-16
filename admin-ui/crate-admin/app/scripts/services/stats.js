'use strict';

angular.module('stats', ['sql'])
  .factory('ClusterState', function ($http, $timeout, $location, $log, SQLQuery) {
    var prefix = $location.search().prefix || '';
    var colorMap = {green: 'label-success',
                    yellow: 'label-warning',
                    red: 'label-danger',
                    '--': 'label-default'};
    var data = {
      name: '--',
      status: '--',
      color_label: 'label-default',
      load: ['-.-', '-.-', '-.-']
    };

    var refreshInterval = 5000;

    var refreshHealth = function() {
      SQLQuery.execute("select sum(number_of_shards) from information_schema.tables").
        success(function(sqlQuery) {
          var configuredShards = 0;
          if (sqlQuery.rowCount > 0) {
            configuredShards = sqlQuery.rows[0][0];
          }
          SQLQuery.execute('select count(*), "primary", state from stats.shards group by "primary", state').
            success(function(sqlQuery) {
              var activePrimaryShards = 0;
              var unassignedShards = 0;
              for (var row in sqlQuery.rows) {
                if (sqlQuery.rows[row][1] == true && sqlQuery.rows[row][2] in {'STARTED':'', 'RELOCATING':''} ) {
                  activePrimaryShards = sqlQuery.rows[row][0];
                } else if (sqlQuery.rows[row][2] == 'UNASSIGNED') {
                  unassignedShards = sqlQuery.rows[row][0];
                }
              }

              if (activePrimaryShards < configuredShards) {
                data.status = 'red';
                data.color_label = colorMap['red'];
              } else if (unassignedShards > 0) {
                data.status = 'yellow';
                data.color_label = colorMap['yellow'];
              } else {
                data.status = 'green';
                data.color_label = colorMap['green'];
              }
            }).
            error(function(sqlQuery) {
              data.status = '--';
              data.color_label = colorMap['--'];

            });
        }).
        error(function(sqlQuery) {
          data.status = '--';
          data.color_label = colorMap['--'];
        });
      $timeout(refreshHealth, refreshInterval);
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
          data.name = res_data.cluster_name;
          data.load = clusterLoad(res_data.nodes);
        }).
        error(function() {
          data.name = '--';
          data.load = ['-.-', '-.-', '-.-'];
        });
      $timeout(refreshState, refreshInterval);
    };

    refreshHealth();
    refreshState();

    return {
      data: data
    };
  });