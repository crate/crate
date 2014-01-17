/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

'use strict';

angular.module('stats', ['sql'])
  .factory('ClusterState', function ($http, $timeout, $location, $log, SQLQuery) {
    var prefix = $location.search().prefix || '';
    var data = {
      name: '--',
      status: '--',
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
              } else if (unassignedShards > 0) {
                data.status = 'yellow';
              } else {
                data.status = 'green';
              }
            }).
            error(function(sqlQuery) {
              data.status = '--';
            });
        }).
        error(function(sqlQuery) {
          data.status = '--';
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