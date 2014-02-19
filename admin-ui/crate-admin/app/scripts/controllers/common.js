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

angular.module('common', ['stats'])
  .controller('StatusBarController', function ($scope, $log, $location, ClusterState) {
    var colorMap = {green: 'label-success',
                    yellow: 'label-warning',
                    red: 'label-danger',
                    '--': 'label-default'};

    $scope.cluster_color_label = 'label-default';


    $scope.$watch( function () { return ClusterState.data; }, function (data) {
      $scope.cluster_state = data.status;
      $scope.cluster_name = data.name;
      $scope.cluster_color_label = colorMap[data.status];
      $scope.load1 = data.load[0] == '-.-' ? data.load[0] : data.load[0].toFixed(2);
      $scope.load5 = data.load[1] == '-.-' ? data.load[1] : data.load[1].toFixed(2);
      $scope.load15 = data.load[2] == '-.-' ? data.load[2] : data.load[2].toFixed(2);
    }, true);

    var prefix = $location.search().prefix || '';
    $scope.docs_url = prefix + "/_plugin/docs";

  })
  .controller('NavigationController', function ($scope, $location, ClusterState) {
    var colorMap = {green: '',
                    yellow: 'label-warning',
                    red: 'label-danger',
                    '--': ''};

    $scope.$watch( function () { return ClusterState.data; }, function (data) {
      $scope.cluster_color_label_bar = colorMap[data.status];
    }, true);


    $scope.isActive = function (viewLocation) {
      if (viewLocation == '/') {
        return viewLocation === $location.path();
      } else {
        return $location.path().substr(0, viewLocation.length) == viewLocation;
      }
    };

    $scope.params = $location.search().prefix ? '?prefix='+$location.search().prefix : '';
  });
