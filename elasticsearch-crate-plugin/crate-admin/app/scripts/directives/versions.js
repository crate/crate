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
      controller: ['$scope', '$element', '$attrs', '$transclude', '$http', function($scope, $element, $attrs, $transclude, $http) {
            $scope.crate = 'crate_version';
            $http({method: 'GET', url: '/'}).
              success(function(data) {
                $scope.es = data.version.number;
              }).
              error(function() {
                $scope.es = '-';
              });
          }]
    };
  });
