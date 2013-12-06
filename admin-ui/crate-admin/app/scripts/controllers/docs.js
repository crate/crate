'use strict';

angular.module('docs', [])
  .controller('DocsController', [ '$scope', '$location', function ($scope, $location) {
    var prefix = $location.search().prefix || '';
    $scope.url = prefix + '/_plugin/docs';
  }]);
