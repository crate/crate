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
            sh './blackbox/.venv/bin/sphinx-build -n -W -c docs/ -b html -E docs/ docs/_out/html'
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
            sh './gradlew --no-daemon --parallel -Dtests.crate.slow=true -PtestForks=8 test checkstyleMain jacocoReport'

            // Upload coverage report to Codecov.
            // https://about.codecov.io/blog/introducing-codecovs-new-uploader/
            sh '''
              # Download uploader program and perform integrity checks.
              curl https://keybase.io/codecovsecurity/pgp_keys.asc | gpg --import # One-time step
              curl -Os https://uploader.codecov.io/latest/linux/codecov
              curl -Os https://uploader.codecov.io/latest/linux/codecov.SHA256SUM
              curl -Os https://uploader.codecov.io/latest/linux/codecov.SHA256SUM.sig
              gpg --verify codecov.SHA256SUM.sig codecov.SHA256SUM
              shasum -a 256 -c codecov.SHA256SUM

              # Invoke program.
              chmod +x codecov
              ./codecov -t ${CODECOV_TOKEN}
            '''.stripIndent()
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
