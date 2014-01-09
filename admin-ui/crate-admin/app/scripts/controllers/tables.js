'use strict';

angular.module('tables', ['stats', 'sql', 'common'])
  .controller('TablesController', function ($scope, $location, $log, $timeout, SQLQuery, roundWithUnitFilter, bytesFilter) {
    var table_name = $location.search().table || '';

    var refreshInterval = 5000;

    var colorMapPanel = {green: 'panel-success',
                    yellow: 'panel-warning',
                    red: 'panel-danger',
                    '--': 'panel-default'};
    var colorMapLabel = {green: '',
                    yellow: 'label-warning',
                    red: 'label-danger',
                    '--': ''};

    var selected_table = $location.search().table || '';

    var empty_table = {
      'name': 'Tables (0 found)',
      'summary': '',
      'health': '--',
      'health_label_class': '',
      'health_panel_class': '',
      'records_total':0,
      'records_replicated':0,
      'records_underreplicated':0,
      'records_unavailable': 0,
      'shards_configured': 0,
      'shards_started': 0,
      'shards_active': 0,
      'shards_missing': 0,
      'shards_underreplicated': 0,
      'replicas_configured': 0,
      'size': 0
    };

    function getData() {
      SQLQuery.execute('select table_name, sum(number_of_shards), sum(number_of_replicas) from information_schema.tables ' +
                           'group by table_name').
        success(function(sqlQuery1){
          SQLQuery.execute('select table_name, sum(num_docs), "primary", avg(num_docs), count(*), state, sum(size) '+
                       'from stats.shards group by table_name, "primary", state ' +
                       'order by table_name, "primary"').
            success(function(sqlQuery2) {
              $scope.renderSidebar = true;
              calculateData(sqlQuery1, sqlQuery2);
            }).
            error(function(sqlQuery) {
              calculateData(sqlQuery1);
            });
        }).
        error(function(sqlQuery) {
          $scope.tables = [];
          $scope.table = angular.copy(empty_table);
          $scope.selected_table = '';
          $scope.renderSidebar = false;
          $scope.renderSchema = false;
        });

      var promise = $timeout(getData, refreshInterval);
      $scope.$on('$destroy', function(){
        $timeout.cancel(promise);
      });
    }

    function calculateData(sqlQuery1, sqlQuery2) {
      var tables = {};
      var tables_list = [];
      // fill table state with response from 1st query
      for (var row in sqlQuery1.rows) {
        var current_row = sqlQuery1.rows[row];
        if (tables[current_row[0]] == undefined) {
          tables[current_row[0]] = angular.copy(empty_table);
        }
        tables[current_row[0]]['shards_configured'] = current_row[1];
        tables[current_row[0]]['replicas_configured'] = current_row[2];
      }

      // fill table state with response from 2nd query
      if (sqlQuery2 != undefined) {
        for (var row in sqlQuery2.rows) {
          var current_row = sqlQuery2.rows[row];
          if (tables[current_row[0]] != undefined) {
            if (current_row[2] == true) {
              tables[current_row[0]]['records_total'] += current_row[1];
              tables[current_row[0]]['avg_docs'] = current_row[3];
              tables[current_row[0]]['size'] += current_row[6];
              tables[current_row[0]]['shards_active'] += current_row[4];
              if (current_row[5] == 'STARTED') {
                tables[current_row[0]]['shards_started'] = current_row[4];
              }
            } else if (current_row[5] != 'UNASSIGNED') {
              tables[current_row[0]]['records_replicated'] = current_row[1];
            } else {
              tables[current_row[0]]['shards_missing'] += current_row[4];
            }
          }
        }
      }

      for (var table_name in tables) {
        var table = tables[table_name];

        table['health'] = 'green';
        table['summary'] = roundWithUnitFilter(table['records_total'], 1) + ' Records (' + bytesFilter(table['size']) + ') / ' +
                           table['replicas_configured'] + ' Replicas / ' + table['shards_configured'] + ' Shards (' + table['shards_started'] + ' Started)';

        if (table['shards_missing'] > 0) {
          table['records_unavailable'] = (table['shards_missing'] * table['avg_docs']).toFixed(0);
          table['health'] = 'red';
          table['summary'] = roundWithUnitFilter(table['records_unavailable'], 1) + ' Unavailable Records / ' + table['summary'];
        }
        if (table['replicas_configured'] > 0 && table['records_total'] != table['records_replicated']) {
          table['records_underreplicated'] = table['records_total'] - table['records_replicated'];
          table['shards_underreplicated'] = (table['records_not_replicated'] / table['avg_docs']).toFixed(1);
          if (table['health'] == 'green') {
            table['summary'] = table['shards_underreplicated'] + ' Underreplicated Shards / ' + table['summary'];
          }
          table['health'] = 'yellow';
        }

        table['health_panel_class'] = colorMapPanel[table['health']];
        table['health_label_class'] = colorMapLabel[table['health']];

        table['name'] = table_name;

        tables_list.push(tables[table_name]);
      }

      if (tables_list.length == 0) {
        $scope.tables = tables_list;
        $scope.table = angular.copy(empty_table);
        $scope.selected_table = '';
        $scope.renderSidebar = false;
        $scope.renderSchema = false;
        return;
      }

      // sort tables by health
      tables_list.sort(compareListByHealth);

      selected_table = $location.search().table || tables_list[0].name;

      $scope.tables = tables_list;
      $scope.table = tables[selected_table];
      $scope.selected_table = selected_table;

      // query for table schema
      SQLQuery.execute("select column_name, data_type from information_schema.columns where table_name = '"+selected_table+"'").
        success(function(sqlQuery){
          $scope.renderSchema = true;
          $scope.schemaHeaders = [];
          for (var col in sqlQuery.cols) {
              $scope.schemaHeaders.push(sqlQuery.cols[col]);
          }

          $scope.schemaRows = sqlQuery.rows;
        }).
        error(function(sqlQuery) {
          $scope.renderSchema = false;
        });

    }

    $scope.isActive = function (table_name) {
      return table_name === selected_table;
    };

    var healthPriorityMap = {green: 2,
                             yellow: 1,
                             red: 0};

    function compareListByHealth(a,b) {
      if (healthPriorityMap[a.health] < healthPriorityMap[b.health])
         return -1;
      if (healthPriorityMap[a.health] > healthPriorityMap[b.health])
        return 1;
      return 0;
    }

    getData();

    // bind tooltips
    $("[rel=tooltip]").tooltip({ placement: 'top'});

    // sidebar button handler (mobile view)
    $scope.toggleSidebar = function() {
      $("#wrapper").toggleClass("active");
    };


  });