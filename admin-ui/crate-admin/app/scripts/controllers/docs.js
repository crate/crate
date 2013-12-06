'use strict';

angular.module('docs', [])
  .controller('DocsController', [ '$scope', '$location', function ($scope, $location) {
    var prefix = $location.search().prefix || '';
    prefix = 'http://localhost:9200';
    $scope.url = prefix + '/_plugin/docs';
  }]);
