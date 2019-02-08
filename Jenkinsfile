pipeline {
  agent any
  stages {
    stage('Build') {
      agent { label 'large' }
      steps {
        step([$class: 'WsCleanup'])
        checkout scm
        sh 'git submodule update --init'
        sh './gradlew --no-daemon --parallel clean compileTestJava'
        stash includes: '**/build/**/*.jar', name: 'crate'
      }
    }
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
        stage('test') {
          agent { label 'large' }
          environment {
            CODECOV_TOKEN = credentials('cratedb-codecov-token')
          }
          steps {
            sh 'git clean -xdff'
            checkout scm
            sh 'git submodule update --init'
            unstash 'crate'
            sh './gradlew --no-daemon --parallel -PtestForks=8 test forbiddenApisMain pmdMain jacocoReport'
            sh 'curl -s https://codecov.io/bash | bash'
          }
          post {
            always {
              junit '*/build/test-results/test/*.xml'
            }
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
            sh 'git submodule update --init'
            unstash 'crate'
            sh './gradlew --no-daemon --parallel -PtestForks=8 test jacocoReport'
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
            sh 'git submodule update --init'
            unstash 'crate'
            sh './gradlew --no-daemon --parallel -PtestForks=8 test jacocoReport'
            sh 'curl -s https://codecov.io/bash | bash'
          }
          post {
            always {
              junit '*/build/test-results/test/*.xml'
            }
          }
        }
        stage('itest jdk8') {
          agent { label 'large' }
          tools {
            jdk 'jdk8'
          }
          steps {
            sh 'git clean -xdff'
            checkout scm
            sh 'git submodule update --init'
            unstash 'crate'
            sh './gradlew --no-daemon itest'
          }
        }
        stage('itest jdk11') {
          agent { label 'large' }
          tools {
            jdk 'jdk11'
          }
          steps {
            sh 'git clean -xdff'
            checkout scm
            sh 'git submodule update --init'
            unstash 'crate'
            sh './gradlew --no-daemon itest'
          }
        }
        stage('itest jdk12') {
          agent { label 'large' }
          tools {
            jdk 'jdk12'
          }
          steps {
            sh 'git clean -xdff'
            checkout scm
            sh 'git submodule update --init'
            unstash 'crate'
            sh './gradlew --no-daemon itest'
          }
        }
        stage('blackbox tests') {
          agent { label 'medium' }
          tools {
            jdk 'jdk11'
          }
          steps {
            sh 'git clean -xdff'
            checkout scm
            sh 'git submodule update --init'
            sh './gradlew --no-daemon hdfsTest monitoringTest gtest'
          }
        }
      }
    }
  }
}
