'use strict';

describe('Controller: SqlconsoleCtrl', function () {

  // load the controller's module
  beforeEach(module('crateAdminApp'));

  var SqlconsoleCtrl,
    scope;

  // Initialize the controller and a mock scope
  beforeEach(inject(function ($controller, $rootScope) {
    scope = $rootScope.$new();
    SqlconsoleCtrl = $controller('SqlconsoleCtrl', {
      $scope: scope
    });
  }));
});
