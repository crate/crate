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
          steps {
            sh 'git clean -xdff'
            checkout scm
            sh 'jabba install openjdk@1.11.0-2'
            sh 'JAVA_HOME=$(jabba which --home openjdk@1.11.0-2) timeout 30m ./gradlew --no-daemon --parallel -PtestForks=8 test forbiddenApisMain jacocoReport'
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
          steps {
            sh 'git clean -xdff'
            checkout scm
            sh 'jabba install openjdk@1.12.0-1'
            sh 'JAVA_HOME=$(jabba which --home openjdk@1.12.0-1) timeout 30m ./gradlew --no-daemon --parallel -PtestForks=8 test jacocoReport'
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
          steps {
            sh 'git clean -xdff'
            checkout scm
            sh 'jabba install openjdk@1.11.0-2'
            sh 'JAVA_HOME=$(jabba which --home openjdk@1.11.0-2) timeout 20m ./gradlew --no-daemon itest'
          }
        }
        stage('ce itest jdk11') {
          agent { label 'medium' }
          steps {
            sh 'git clean -xdff'
            checkout scm
            sh 'jabba install openjdk@1.11.0-2'
            sh 'JAVA_HOME=$(jabba which --home openjdk@1.11.0-2) timeout 20m ./gradlew --no-daemon ceItest'
          }
        }
        stage('ce licenseTest jdk11') {
          agent { label 'medium' }
          steps {
            sh 'git clean -xdff'
            checkout scm
            sh 'jabba install openjdk@1.11.0-2'
            sh 'JAVA_HOME=$(jabba which --home openjdk@1.11.0-2) timeout 20m ./gradlew --no-daemon ceLicenseTest'
          }
        }
        stage('itest jdk12') {
          agent { label 'medium' }
          steps {
            sh 'git clean -xdff'
            checkout scm
            sh 'jabba install openjdk@1.12.0-1'
            sh 'JAVA_HOME=$(jabba which --home openjdk@1.12.0-1) timeout 20m ./gradlew --no-daemon itest'
          }
        }
        stage('itest adopt-jdk-12') {
          agent { label 'medium' }
          steps {
            sh 'git clean -xdff'
            checkout scm
            sh 'jabba install adopt@1.12.33-0'
            sh 'JAVA_HOME=$(jabba which --home adopt@1.12.33-0) timeout 20m ./gradlew --no-daemon itest'
          }
        }
        stage('blackbox tests') {
          agent { label 'medium' }
          steps {
            sh 'git clean -xdff'
            checkout scm
            sh 'jabba install openjdk@1.11.0-2'
            sh 'JAVA_HOME=$(jabba which --home openjdk@1.11.0-2) timeout 20m ./gradlew --no-daemon hdfsTest monitoringTest gtest dnsDiscoveryTest'
          }
        }
      }
    }
  }
}
