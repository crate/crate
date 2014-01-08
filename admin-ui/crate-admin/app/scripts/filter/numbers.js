
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
  })
  .filter('bytes', function() {
   	return function(bytes, precision) {
   		if (bytes == 0 || isNaN(parseFloat(bytes)) || !isFinite(bytes)) return '-';
   		if (typeof precision === 'undefined') precision = 1;
   		var units = ['bytes', 'kB', 'MB', 'GB', 'TB', 'PB'],
   			number = Math.floor(Math.log(bytes) / Math.log(1024));
   		return (bytes / Math.pow(1024, Math.floor(number))).toFixed(precision) +  ' ' + units[number];
   	}
   });
