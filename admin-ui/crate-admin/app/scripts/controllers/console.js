/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

'use strict';

angular.module('console', ['sql'])
  .controller('ConsoleController', function ($scope, $http, $location, SQLQuery, $log) {

    $scope.statement = "";
    $scope.rows = [];

    $('iframe').hide();

    $scope.resultHeaders = [];
    $scope.renderTable = false;
    $scope.error = {};
    $scope.error.hide = true;

    var loadingIndicator = Ladda.create(document.querySelector('button[type=submit]'));

    $scope.execute = function() {
      loadingIndicator.start();
      SQLQuery.execute($scope.statement).
        success(function(sqlQuery) {
          loadingIndicator.stop();
          $scope.error.hide = true;
          $scope.renderTable = true;

          $scope.resultHeaders = [];
          for (var col in sqlQuery.cols) {
              $scope.resultHeaders.push(sqlQuery.cols[col]);
          }

          $scope.rows = sqlQuery.rows;
          $scope.status = sqlQuery.status();
        }).
        error(function(sqlQuery) {
          loadingIndicator.stop();
          $scope.error.hide = false;
          $scope.renderTable = false;
          $scope.error.message = sqlQuery.error.message;
          $scope.status = sqlQuery.status();
          $scope.rows = [];
          $scope.resultHeaders = [];
        });
    };

  });
