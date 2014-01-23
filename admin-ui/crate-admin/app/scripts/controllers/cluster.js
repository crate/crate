'use strict';

angular.module('cluster', ['stats', 'sql', 'common'])
  .controller('ClusterController', function ($scope, $location, $log, $timeout, $routeParams,
                                             $http, $filter) {

    var refreshInterval = 5000;

    $scope.percentageLimitYellow = 90;
    $scope.percentageLimitRed = 98;

    var colorMapPanel = {green: 'panel-success',
                    yellow: 'panel-warning',
                    red: 'panel-danger',
                    '--': 'panel-default'};
    var colorMapLabel = {green: '',
                    yellow: 'label-warning',
                    red: 'label-danger',
                    '--': ''};

    var selected_node = $routeParams.node_name || '';

    var empty_node = {
      'name': 'Cluster (0 Nodes)',
      'id': '',
      'summary': [],
      'health': '--',
      'health_label_class': '',
      'health_panel_class': '',
      'hostname': '',
      'address': '',
      'heap': {
        'total': 0,
        'used': 0,
        'used_percent': 0
      },
      'fs': {
        'free': 0,
        'used': 0,
        'free_percent': 0,
        'used_percent': 0
      }
    };

    var prefix = $location.search().prefix || '';

    function loadNodesStats() {
      $http({method: 'GET', url: prefix + '/_nodes/stats?all=true'}).
        success(function(res_data) {
          $scope.renderSidebar = true;
          processNodesStats(res_data);
        }).
        error(function() {
          $scope.renderSidebar = false;
          processNodesStats({});
        });

      var promise = $timeout(loadNodesStats, refreshInterval);
      $scope.$on('$destroy', function(){
        $timeout.cancel(promise);
      });

    }

    function processNodesStats(result) {
      var nodes = {};
      var nodes_list = [];

      if (result['nodes'] != undefined) {
        for (var nodeId in result.nodes) {
          var node = result.nodes[nodeId];
          var nodeName = node.name;

          if (nodes[nodeName] == undefined) {
            nodes[nodeName] = angular.copy(empty_node);
          }

          nodes[nodeName]['id'] = nodeId;
          nodes[nodeName]['name'] = nodeName;
          nodes[nodeName]['hostname'] = node.hostname;

          if (node['attributes'] != undefined) {
            var address_parts = node.attributes.http_address.split(':');
            if (address_parts.length > 2) {
              nodes[nodeName]['address'] = ["http", "//"+node.hostname, address_parts[2]].join(':');
            }
          }

          nodes[nodeName]['heap'] = {
            'total': node.jvm.mem.heap_max_in_bytes,
            'used': node.jvm.mem.heap_used_in_bytes,
            'used_percent': node.jvm.mem.heap_used_percent
          };

          nodes[nodeName]['fs'] = {
            'free': node.fs.total.free_in_bytes,
            'used': node.indices.store.size_in_bytes,
            'free_percent': (node.fs.total.free_in_bytes / node.fs.total.total_in_bytes) * 100,
            'used_percent': (node.indices.store.size_in_bytes / node.fs.total.total_in_bytes) * 100
          }

          if (nodes[nodeName]['heap']['used_percent'] >= $scope.percentageLimitYellow) {
            nodes[nodeName]['summary'].push("Used HEAP " + $filter('number')(nodes[nodeName]['heap']['used_percent'], 0) + "%");
            nodes[nodeName]['health'] = 'yellow';
            if (nodes[nodeName]['heap']['used_percent'] >= $scope.percentageLimitRed) {
              nodes[nodeName]['health'] = 'red';
            }
          }
          if (nodes[nodeName]['fs']['used_percent'] >= $scope.percentageLimitYellow) {
            nodes[nodeName]['summary'].push("Used Disk " + $filter('number')(nodes[nodeName]['fs']['used_percent'], 0) + "%");
            nodes[nodeName]['health'] = 'yellow';
            if (nodes[nodeName]['fs']['used_percent'] >= $scope.percentageLimitRed) {
              nodes[nodeName]['health'] = 'red';
            }
          }

          nodes[nodeName]['health_panel_class'] = colorMapPanel[nodes[nodeName]['health']];
          nodes[nodeName]['health_label_class'] = colorMapLabel[nodes[nodeName]['health']];

          nodes_list.push(nodes[nodeName]);

        }
      }

      if (nodes_list.length == 0) {
        $scope.nodes_list = nodes_list;
        $scope.node = angular.copy(empty_node);
        $scope.selected_node = '';
        $scope.renderSidebar = false;
        return;
      }

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

      // sort nodes by health
      nodes_list.sort(compareListByHealth);

      if ($routeParams.node_name && nodes[$routeParams.node_name] != undefined) {
        selected_node = $routeParams.node_name;
      } else {
        selected_node = nodes_list[0].name;
      }

      $scope.nodes = nodes_list;
      $scope.node = nodes[selected_node];
      $scope.selected_node = selected_node;

    }

    loadNodesStats();


    $scope.isActive = function (node_name) {
      return node_name === selected_node;
    };

    // bind tooltips
    $("[rel=tooltip]").tooltip({ placement: 'top'});

    // sidebar button handler (mobile view)
    $scope.toggleSidebar = function() {
      $("#wrapper").toggleClass("active");
    };

    // additional route params
    $scope.routeParams = $location.search().prefix ? '?prefix='+$location.search().prefix : '';

  });