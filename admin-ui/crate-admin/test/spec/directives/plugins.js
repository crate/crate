'use strict';

describe('Directive: plugins', function () {
  beforeEach(module('crateAdminApp'));

  var element;

  it('should make hidden element visible', inject(function ($rootScope, $compile) {
    element = angular.element('<plugins></plugins>');
    element = $compile(element)($rootScope);
    expect(element.text()).toContain("plugin in plugins");
  }));
});
