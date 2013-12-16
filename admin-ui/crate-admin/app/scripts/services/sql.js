'use strict';

angular.module('sql', [])
  .factory('SQLQuery', function ($http, $location, $log, $q) {
    var prefix = $location.search().prefix || '';

    var SQLQuery = function(response, failed) {
      this.rows = [];
      this.cols = [];
      this.rowCount = [];
      this.duration = 0;
      this.error = {'message':'', code:0};
      this.failed = failed;

      if (response.error != undefined || this.failed == true) {
        this.failed = true;
        this.error = response.error;
      } else {
        this.rows = response.rows;
        this.cols = response.cols;
        this.rowCount = response.rowCount;
        this.duration = response.duration;
      }

    };

    SQLQuery.execute = function(stmt, args) {
      var data = {'stmt': stmt};
      if (args != undefined) {
        data.args = args;
      }
      var deferred = $q.defer();
      var promise = deferred.promise;

      promise.success = function(fn) {
        promise.then(function(sqlQuery) {
          fn(sqlQuery);
        });
        return promise;
      };

      promise.error = function(fn) {
        promise.then(null, function(sqlQuery) {
          fn(sqlQuery);
        });
        return promise;
      };

      $http.post(prefix + '/_sql', data).
        success(function(data, status, headers, config) {
          deferred.resolve(new SQLQuery(data, false));
        }).
        error(function(data, status, headers, config) {
          $log.debug("Got ERROR response from query: " + stmt + " with status: " + status);
          if (status == 0) {
            data = {'error': {'message': 'Connection error', 'code':0}};
          }
          deferred.reject(new SQLQuery(data, true));
        });

      return promise;
    };

    return SQLQuery;
  });
