'use strict';

angular.module('crateAdminApp')
  .directive('health', function () {
    return {
      template: '<div class="pull-right ng-cloak">' +
                '  <span class="badge {{color}} cluster-state">' +
                '    {{nodes}}, {{shards}}' +
                '  </span>' +
                '</div>',
      restrict: 'E',
      controller: ['$scope', '$element', '$attrs', '$transclude', '$http', '$timeout', '$location', function($scope, $element, $attrs, $transclude, $http, $timeout, $location) {
          var prefix = $location.search().prefix || '';
          var colorMap = {green: 'badge-success',
                          yellow: 'badge-warning',
                          red: 'badge-important'};
          var refresh = function() {
            $http({method: 'GET', url: prefix + '/_cluster/health'}).
              success(function(data) {
                $scope.color = colorMap[data.status];
                /* jshint -W106 */
                $scope.nodes = data.number_of_nodes;
                $scope.shards = data.active_primary_shards;
                /* jshint +W106 */
              }).
              error(function() {
                $scope.color = '';
                $scope.nodes = '-';
                $scope.shards = '-';
              });
            $timeout(refresh, 5000);
          };
          refresh();
        }]
    };
  });
