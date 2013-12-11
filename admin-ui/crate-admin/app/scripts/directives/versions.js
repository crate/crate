'use strict';

angular.module('crateAdminApp')
  .directive('versions', function () {
    return {
      template: '<div class="pull-right ng-cloak versions">' +
                '  <table class="table">' +
                '    <tr><td>crate:</td><td>{{crate}}</td></tr>' +
                '    <tr><td>elasticsearch:</td><td>{{es}}</td></tr>' +
                '  </table>' +
                '</div>',
      restrict: 'E',
      controller: ['$scope', '$element', '$attrs', '$transclude', '$http', '$location', function($scope, $element, $attrs, $transclude, $http, $location) {
            var prefix = $location.search().prefix || '';
            $http({method: 'GET', url: prefix + '/'}).
              success(function(data) {
                $scope.crate = data.version.number;
                $scope.es = data.version.es_version;
              }).
              error(function() {
                $scope.crate = '-';
                $scope.es = '-';
              });
          }]
    };
  });
