'use strict';

angular.module('overview', ['stats', 'sql', 'common'])
  .controller('OverviewController', function ($scope, $log, $timeout, ClusterState, SQLQuery) {
    var colorMap = {green: 'panel-success',
                    yellow: 'panel-warning',
                    red: 'panel-danger',
                    '--': 'panel-default'};

    $scope.available_data = '--';
    $scope.records_unavailable = '--';
    $scope.replicated_data = '--';
    $scope.records_total = '--';
    $scope.records_underreplicated = '--';
    $scope.cluster_state = '--';
    $scope.cluster_color_class = 'panel-default';

    $scope.$watch( function () { return ClusterState.data; }, function (data) {
      $scope.cluster_state = data.status;
      $scope.cluster_color_class = colorMap[data.status];
    }, true);

    var refreshInterval = 5000;

    function getData() {
      SQLQuery.execute('select table_name, sum(num_docs), "primary", relocating_node, avg(num_docs), count(*), state '+
                       'from stats.shards group by table_name, "primary", relocating_node, state ' +
                       'order by table_name, "primary"').
        success(function(sqlQuery1){
          SQLQuery.execute('select table_name, sum(number_of_shards) from information_schema.tables ' +
                           'group by table_name').
            success(function(sqlQuery2) {
              calculateData(sqlQuery1, sqlQuery2);
            }).
            error(function(sqlQuery) {
              $log.error("Error occurred on SQL query: " + sqlQuery.error.message);
              calculateData(sqlQuery1);
              $scope.available_data = '--';
              $scope.records_unavailable = '--';
            });
        }).
        error(function(sqlQuery) {
          $log.error("Error occurred on SQL query: " + sqlQuery.error.message);
          $scope.available_data = '--';
          $scope.records_unavailable = '--';
          $scope.replicated_data = '--';
          $scope.records_total = '--';
          $scope.records_underreplicated = '--';
        });

      var promise = $timeout(getData, refreshInterval);
      $scope.$on('$destroy', function(){
        $timeout.cancel(promise);
      });
    }

    function calculateData(sqlQuery1, sqlQuery2) {
      var records_total = 0;
      var records_not_replicated = 0;
      var records_unavailable = 0;
      var table_state = {};

      // fill table state with response from 1st query
      for (var row in sqlQuery1.rows) {
        var current_row = sqlQuery1.rows[row];
        if (table_state[current_row[0]] == undefined) {
          table_state[current_row[0]] = {'total':0, 'replicated': -1, 'avg_docs': 0, 'active_shards': 0}
        }
        if (current_row[2] == true) {
          table_state[current_row[0]]['total'] = current_row[1];
          table_state[current_row[0]]['avg_docs'] += current_row[4];
          table_state[current_row[0]]['active_shards'] += current_row[5];
        } else if (current_row[6] != 'UNASSIGNED') {
          table_state[current_row[0]]['replicated'] = current_row[1];
        }
      }
      // fill table state with response from 2st query
      if (sqlQuery2 != undefined) {
        for (var row in sqlQuery2.rows) {
          var current_row = sqlQuery2.rows[row];
          if (table_state[current_row[0]] == undefined) {
            table_state[current_row[0]] = {'total_shards':0}
          }
          table_state[current_row[0]]['total_shards'] = current_row[1];
        }
      }

      // calculated cluster numbers
      for (var table in table_state) {
        records_total += table_state[table]['total'];
        if (table_state[table]['replicated'] > -1) {
          records_not_replicated += (table_state[table]['total'] - table_state[table]['replicated']);
        }
        var unavailable_shards = table_state[table]['total_shards'] - table_state[table]['active_shards'];
        if (unavailable_shards > 0) {
          records_unavailable = unavailable_shards * table_state[table]['avg_docs'];
        }
      }

      $scope.records_total = records_total;

      if (records_not_replicated == 0) {
        $scope.records_underreplicated = 0;
        $scope.replicated_data = '100%';
      } else {
        $scope.records_underreplicated = records_not_replicated.toFixed(0);
        $scope.replicated_data = (100-((records_not_replicated/records_total)*100)).toFixed(2) + '%';
      }

      if (records_unavailable == 0) {
        $scope.records_unavailable = 0;
        $scope.available_data = '100%';
      } else {
        $scope.records_unavailable = records_unavailable.toFixed(0);
        $scope.available_data = (100-((records_unavailable/records_total)*100)).toFixed(2) + '%';
      }

    }

    getData();

    // bind tooltips
    $("[rel=tooltip]").tooltip({ placement: 'top'});

  });
