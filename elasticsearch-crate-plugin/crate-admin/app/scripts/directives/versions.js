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
                $scope.es = data.version.number;
              }).
              error(function() {
                $scope.es = '-';
              });
            $http({method: 'GET', url: prefix + '/_plugin/crate-admin/version.json'}).
              success(function(data) {
                $scope.crate = data.version.crate;
              }).
              error(function() {
                $scope.crate = '-';
              });
          }]
    };
  });
