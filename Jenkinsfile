pipeline {
  agent any
  tools {
    // used to run gradle
    jdk 'jdk11'
  }
  options {
    timeout(time: 45, unit: 'MINUTES') 
  }
  environment {
    CI_RUN = 'true'
  }
  stages {
    stage('Parallel') {
      parallel {
        stage('sphinx') {
          agent { label 'medium' }
          steps {
            sh 'cd ./blackbox/ && ./bootstrap.sh'
            sh './blackbox/.venv/bin/sphinx-build -n -W -c docs/ -b html -E docs/ docs/out/html'
            sh 'find ./blackbox/*/src/ -type f -name "*.py" | xargs ./blackbox/.venv/bin/pycodestyle'
          }
        }
        stage('Java tests') {
          agent { label 'large' }
          environment {
            CODECOV_TOKEN = credentials('cratedb-codecov-token')
          }
          steps {
            sh 'git clean -xdff'
            checkout scm
            sh './gradlew --no-daemon --parallel -PtestForks=8 test checkstyleMain forbiddenApisMain jacocoReport'
            sh 'curl -s https://codecov.io/bash | bash'
          }
          post {
            always {
              junit '**/build/test-results/test/*.xml'
            }
          }
        }
        stage('itest') {
          agent { label 'medium' }
          steps {
            sh 'git clean -xdff'
            checkout scm
            sh 'python3 ./blackbox/kill_4200.py'
            sh './gradlew --no-daemon itest'
          }
        }
        stage('ce itest') {
          agent { label 'medium' }
          steps {
            sh 'git clean -xdff'
            checkout scm
            sh 'python3 ./blackbox/kill_4200.py'
            sh './gradlew --no-daemon ceItest'
          }
        }
        stage('ce licenseTest') {
          agent { label 'medium' }
          steps {
            sh 'git clean -xdff'
            checkout scm
            sh './gradlew --no-daemon ceLicenseTest'
          }
        }
        stage('blackbox tests') {
          agent { label 'medium' }
          steps {
            sh 'git clean -xdff'
            checkout scm
            sh './gradlew --no-daemon hdfsTest s3Test monitoringTest gtest dnsDiscoveryTest sslTest'
          }
        }
      }
    }
  }
}
