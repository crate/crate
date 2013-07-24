'use strict';

angular.module('crateAdminApp')
  .controller('MainCtrl', function () {

	  $scope.deactivate_iframe = function() {
		  alert('test');
		  $('iframe').attr('src', '');
	  }
});
