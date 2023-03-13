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
          agent { label 'small' }
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
            sh './gradlew --no-daemon --parallel -Dtests.crate.slow=true -PtestForks=8 test jacocoReport'

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
            sh './gradlew --no-daemon s3Test monitoringTest gtest dnsDiscoveryTest sslTest'
          }
        }
        stage('benchmarks') {
          agent { label 'medium' }
          steps {
            sh 'git clean -xdff'
            checkout scm
            sh 'git clone https://github.com/crate/crate-benchmarks'
            sh '''
              ./gradlew clean distTar
              rm -rf crate-dist
              mkdir crate-dist
              tar xvz --strip-components=1 -f app/build/distributions/crate-*.tar.gz -C crate-dist/
              export JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
              export CR8_NO_TQDM=True
              cd crate-benchmarks
              python3 -m venv venv
              venv/bin/python -m pip install -U wheel
              venv/bin/python -m pip install -r requirements.txt
              venv/bin/python compare_run.py \
                --v1 branch:master \
                --v2 ../app/build/distributions/crate-*.tar.gz \
                --spec fast/queries.toml \
                --env CRATE_HEAP_SIZE=2g \
                --forks 1 \
                --show-plot true
            '''.stripIndent()
          }
        }
      }
    }
  }
}
