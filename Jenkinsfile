pipeline {
  agent any
  stages {
    stage('Parallel') {
      parallel {
        stage('sphinx') {
          agent { label 'medium' }
          steps {
            sh 'cd ./blackbox/ && ./bootstrap.sh'
            sh './blackbox/.venv/bin/sphinx-build -n -W -c docs/ -b html -E blackbox/docs/ docs/out/html'
            sh 'find ./blackbox/*/src/ -type f -name "*.py" | xargs ./blackbox/.venv/bin/pycodestyle'
          }
        }
        stage('test jdk11') {
          agent { label 'large' }
          environment {
            CODECOV_TOKEN = credentials('cratedb-codecov-token')
          }
          tools {
            jdk 'jdk11'
          }
          steps {
            sh 'git clean -xdff'
            checkout scm
            sh './gradlew --no-daemon --parallel -PtestForks=8 test forbiddenApisMain jacocoReport'
            sh 'curl -s https://codecov.io/bash | bash'
          }
          post {
            always {
              junit '*/build/test-results/test/*.xml'
            }
          }
        }
        stage('test jdk12') {
          agent { label 'large' }
          environment {
            CODECOV_TOKEN = credentials('cratedb-codecov-token')
          }
          tools {
            jdk 'jdk12'
          }
          steps {
            sh 'git clean -xdff'
            checkout scm
            sh './gradlew --no-daemon --parallel -PtestForks=8 test jacocoReport'
            sh 'curl -s https://codecov.io/bash | bash'
          }
          post {
            always {
              junit '*/build/test-results/test/*.xml'
            }
          }
        }
        stage('itest jdk11') {
          agent { label 'medium' }
          tools {
            jdk 'jdk11'
          }
          steps {
            sh 'git clean -xdff'
            checkout scm
            sh './gradlew --no-daemon itest'
          }
        }
        stage('ce itest jdk11') {
          agent { label 'medium' }
          tools {
            jdk 'jdk11'
          }
          steps {
            sh 'git clean -xdff'
            checkout scm
            sh './gradlew --no-daemon ceItest'
          }
        }
        stage('itest jdk12') {
          agent { label 'medium' }
          tools {
            jdk 'jdk12'
          }
          steps {
            sh 'git clean -xdff'
            checkout scm
            sh './gradlew --no-daemon itest'
          }
        }
        stage('blackbox tests') {
          agent { label 'large' }
          tools {
            jdk 'jdk11'
          }
          steps {
            sh 'git clean -xdff'
            checkout scm
            sh 'timeout 20m ./gradlew --no-daemon hdfsTest monitoringTest gtest'
          }
        }
      }
    }
  }
}
