'use strict';

angular.module('sql', [])
  .factory('SQLQuery', function ($http, $location, $log, $q) {
    var prefix = $location.search().prefix || '';

    function SQLQuery(stmt, response, failed) {
      this.stmt = stmt;
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
    }

    SQLQuery.prototype.status = function() {
      var status_string = "";
      var stmt_parts = this.stmt.split(' ');
      var cmd = stmt_parts[0].toUpperCase();
      if (cmd in {'CREATE':'', 'DROP':''}) {
        cmd = cmd + " " + stmt_parts[1].toUpperCase();
      }

      if (this.failed == false) {
        status_string = cmd + " OK (" + (this.duration/1000).toFixed(3) + " sec)";
      } else {
        status_string = cmd + " ERROR (" + (this.duration/1000).toFixed(3) + " sec)";
      }

      $log.debug("Query status: " + status_string);
      return status_string;
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
          deferred.resolve(new SQLQuery(stmt, data, false));
        }).
        error(function(data, status, headers, config) {
          $log.debug("Got ERROR response from query: " + stmt + " with status: " + status);
          if (status == 0) {
            data = {'error': {'message': 'Connection error', 'code':0}};
          }
          deferred.reject(new SQLQuery(stmt, data, true));
        });

      return promise;
    };

    return SQLQuery;
  });
