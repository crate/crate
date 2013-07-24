'use strict';

angular.module('crateAdminApp')
  .directive('plugins', function () {
    return {
      template: '<li ng-repeat="plugin in plugins">' +
                '  <a href="{{plugin.url}}">{{plugin.title}}</a>' +
                '</li>',
      restrict: 'E',
      replace: true,
      controller: ['$scope', '$element', '$attrs', '$transclude', '$location', function($scope, $element, $attrs, $transclude, $location) {
            var prefix = $location.search().prefix || '';
            $scope.plugins = [{'url': prefix + '/_plugin/head',
                               'title': 'Head'},
                              {'url': prefix + '/_plugin/bigdesk',
                               'title': 'BigDesk'},
                              {'url': prefix + '/_plugin/segmentspy',
                               'title': 'SegmentSpy'},
                              {'url': prefix + '/_plugin/docs',
                               'title': 'Documentation'},
                             ];
          }]
    };
  });
