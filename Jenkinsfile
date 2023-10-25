pipeline {
  agent any
  tools {
    // used to run maven
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
          agent { label 'small' }
          steps {
            sh './blackbox/bin/sphinx'
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
            sh './mvnw compile'
            sh '''
              x=(~/.m2/jdks/jdk-$(./mvnw help:evaluate -Dexpression=versions.jdk -q -DforceStdout)*); JAVA_HOME="$x/" ./mvnw test \
                -DforkCount=8 \
                -DthreadCount=2 \
                -Dcheckstyle.skip \
                -Dforbiddenapis.skip=true \
                -Dmaven.javadoc.skip=true \
                -Dtests.crate.slow=true \
                jacoco:report
            '''.stripIndent()

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
              junit '**/target/surefire-reports/*.xml'
            }
          }
        }
        stage('itest') {
          agent { label 'medium && x64' }
          steps {
            sh 'git clean -xdff'
            checkout scm
            sh 'python3 ./blackbox/kill_4200.py'
            sh './blackbox/bin/test-docs'
          }
        }
        stage('blackbox tests') {
          agent { label 'medium && x64' }
          steps {
            sh 'git clean -xdff'
            checkout scm
            sh './blackbox/bin/test test_decommission test_dns_discovery test_jmx test_s3 test_ssl'
          }
        }
      }
    }
  }
}
