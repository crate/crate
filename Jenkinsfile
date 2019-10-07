pipeline {
  agent any
  options {
    timeout(time: 45, unit: 'MINUTES') 
  }
  environment {
    CI_RUN = 'true'
    JDK_11 = 'openjdk@1.11.0'
    JDK_12 = 'openjdk@1.12.0'
    JDK_13 = 'openjdk@1.13.0'
    ADOPT_JDK_12 = 'adopt@1.12.0-2'
  }
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
            sh 'jabba install $JDK_11'
            sh 'JAVA_HOME=$(jabba which --home $JDK_11) ./gradlew --no-daemon --parallel -PtestForks=8 test forbiddenApisMain jacocoReport'
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
            sh 'jabba install $JDK_12'
            sh 'JAVA_HOME=$(jabba which --home $JDK_12) ./gradlew --no-daemon --parallel -PtestForks=8 test jacocoReport'
            sh 'curl -s https://codecov.io/bash | bash'
          }
          post {
            always {
              junit '*/build/test-results/test/*.xml'
            }
          }
        }
        stage('test jdk13') {
          agent { label 'large' }
          environment {
            CODECOV_TOKEN = credentials('cratedb-codecov-token')
          }
          steps {
            sh 'git clean -xdff'
            checkout scm
            sh 'jabba install $JDK_13'
            sh 'JAVA_HOME=$(jabba which --home $JDK_13) ./gradlew --no-daemon --parallel -PtestForks=8 test jacocoReport'
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
            sh 'jabba install $JDK_11'
            sh 'JAVA_HOME=$(jabba which --home $JDK_11) ./gradlew --no-daemon itest'
          }
        }
        stage('ce itest jdk11') {
          agent { label 'medium' }
          steps {
            sh 'git clean -xdff'
            checkout scm
            sh 'jabba install $JDK_11'
            sh 'JAVA_HOME=$(jabba which --home $JDK_11) ./gradlew --no-daemon ceItest'
          }
        }
        stage('ce licenseTest jdk11') {
          agent { label 'medium' }
          steps {
            sh 'git clean -xdff'
            checkout scm
            sh 'jabba install $JDK_11'
            sh 'JAVA_HOME=$(jabba which --home $JDK_11) ./gradlew --no-daemon ceLicenseTest'
          }
        }
        stage('itest jdk12') {
          agent { label 'medium' }
          steps {
            sh 'git clean -xdff'
            checkout scm
            sh 'jabba install $JDK_12'
            sh 'JAVA_HOME=$(jabba which --home $JDK_12) ./gradlew --no-daemon itest'
          }
        }
        stage('itest jdk13') {
          agent { label 'medium' }
          steps {
            sh 'git clean -xdff'
            checkout scm
            sh 'jabba install $JDK_13'
            sh 'JAVA_HOME=$(jabba which --home $JDK_13) ./gradlew --no-daemon itest'
          }
        }
        stage('itest adopt-jdk-12') {
          agent { label 'medium' }
          steps {
            sh 'git clean -xdff'
            checkout scm
            sh 'jabba install $ADOPT_JDK_12'
            sh 'JAVA_HOME=$(jabba which --home $ADOPT_JDK_12) ./gradlew --no-daemon itest'
          }
        }
        stage('blackbox tests') {
          agent { label 'medium' }
          steps {
            sh 'git clean -xdff'
            checkout scm
            sh 'jabba install $JDK_11'
            sh 'JAVA_HOME=$(jabba which --home $JDK_11) ./gradlew --no-daemon hdfsTest monitoringTest gtest dnsDiscoveryTest'
          }
        }
      }
    }
  }
}
