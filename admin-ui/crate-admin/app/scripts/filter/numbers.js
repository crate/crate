
angular.module('common')
  .filter('roundWithUnit', function($filter) {
    return function(input, fraction) {
      if (fraction == undefined) {
        fraction = 3;
      }
      return Math.abs(Number(input)) >= 1.0e+9
           ? $filter('number')(Math.abs(Number(input)) / 1.0e+9, fraction) + " Billion"
           : Math.abs(Number(input)) >= 1.0e+6
           ? $filter('number')(Math.abs(Number(input)) / 1.0e+6, fraction) + " Million"
           : Math.abs(Number(input));
    }
  });
