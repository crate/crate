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
        stage('Java tests') {
          agent { label 'large' }
          environment {
            CODECOV_TOKEN = credentials('cratedb-codecov-token')
          }
          steps {
            sh 'git clean -xdff'
            checkout scm

            sh '''
              ./gradlew :server:test --tests "io.crate.integrationtests.LogicalReplicationITest.test_subscription_state_order*" -Dtests.seed=41B62097D26787C1 -Dtests.locale=asa-TZ -Dtests.timezone=Europe/Warsaw -Dtests.iters=20 --fail-fast
              while [ $? -ne 0 ]; do
                ./gradlew :server:test --tests "io.crate.integrationtests.LogicalReplicationITest.test_subscription_state_order*" -Dtests.seed=41B62097D26787C1 -Dtests.locale=asa-TZ -Dtests.timezone=Europe/Warsaw -Dtests.iters=20 --fail-fast
              done
            '''.stripIndent()
          }
          post {
            always {
              junit '**/build/test-results/test/*.xml'
            }
          }
        }
      }
    }
  }
}
