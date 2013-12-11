'use strict';

describe('Directive: health', function () {
  beforeEach(module('crateAdminApp'));

  var element;

  it('should make hidden element visible', inject(function ($rootScope, $compile) {
    element = angular.element('<health></health>');
  }));
});
