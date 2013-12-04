'use strict';

angular.module('crateAdminApp')
  .directive('plugins', function () {
    return {
      template: '<li ng-repeat="plugin in plugins">' +
                '  <a ng-click="activate_iframe()" href="{{plugin.url}}" target="plugin_frame">{{plugin.title}}</a>' +
                '</li>',
      restrict: 'E',
      replace: true,
      controller: ['$scope', '$element', '$attrs', '$transclude', '$location', function($scope, $element, $attrs, $transclude, $location) {
            var prefix = $location.search().prefix || '';

			$scope.activate_iframe = function() {
				$('iframe').show();
				$('div.content').hide();
			}

            $scope.plugins = [{'url': prefix + '/_plugin/head',
                               'title': 'Head'},
                              {'url': prefix + '/_plugin/bigdesk',
                               'title': 'BigDesk'},
                              {'url': prefix + '/_plugin/segmentspy',
                               'title': 'SegmentSpy'},
                              {'url': prefix + '/_plugin/docs',
                               'title': 'Docs'},
                             ];
          }]
    };
  });
