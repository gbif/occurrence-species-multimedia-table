@Library('gbif-common-jenkins-pipelines') _

pipeline {
  agent any
  tools {
    maven 'Maven 3.9.9'
    jdk 'OpenJDK17'
  }
  options {
    buildDiscarder(logRotator(numToKeepStr: '5'))
    skipStagesAfterUnstable()
    timestamps()
    disableConcurrentBuilds()
  }
  triggers {
    snapshotDependencies()
  }
  parameters {
    separator(name: "release_separator", sectionHeader: "Release Main Project Parameters")
    booleanParam(name: 'RELEASE', defaultValue: false, description: 'Do a Maven release')
    string(name: 'RELEASE_VERSION', defaultValue: '', description: 'Release version (optional)')
    string(name: 'DEVELOPMENT_VERSION', defaultValue: '', description: 'Development version (optional)')
    booleanParam(name: 'DRY_RUN_RELEASE', defaultValue: false, description: 'Dry Run Maven release')
  }
  environment {
     INITIAL_POM_VERSION = readMavenPom().getVersion()
  }
  stages {
    stage('Maven build') {
       when {
        allOf {
          not { expression { params.RELEASE } };
        }
      }
      steps {
        withMaven(
            globalMavenSettingsConfig: 'org.jenkinsci.plugins.configfiles.maven.GlobalMavenSettingsConfig1387378707709',
            mavenSettingsConfig: 'org.jenkinsci.plugins.configfiles.maven.MavenSettingsConfig1396361652540',
            traceability: true) {
                sh '''
                   mvn -B -U clean package dependency:analyze deploy
                  '''
              }
      }
    }

    stage('Maven release') {
      when {
          allOf {
              expression { params.RELEASE };
              branch 'master';
          }
      }
      environment {
          RELEASE_ARGS = utils.createReleaseArgs(params.RELEASE_VERSION, params.DEVELOPMENT_VERSION, params.DRY_RUN_RELEASE)
      }
      steps {
          withMaven(
            globalMavenSettingsConfig: 'org.jenkinsci.plugins.configfiles.maven.GlobalMavenSettingsConfig1387378707709',
            mavenSettingsConfig: 'org.jenkinsci.plugins.configfiles.maven.MavenSettingsConfig1396361652540',
            traceability: true) {
            configFileProvider([
                            configFile(fileId: 'org.jenkinsci.plugins.configfiles.maven.GlobalMavenSettingsConfig1387378707709',
                                                  variable: 'MAVEN_SETTINGS_XML')
                          ]) {
                git 'https://github.com/gbif/occurrence-species-multimedia-table.git'
                sh 'mvn -s $MAVEN_SETTINGS_XML -B release:prepare release:perform $RELEASE_ARGS'
              }
            }
      }
    }
    stage('Build and publish Docker image') {
      when { expression { !params.DRY_RUN_RELEASE } }
      steps {
        script {
          // for release builds use the initial pom version (release plugin will have bumped the POM later)
          def versionToBuild
          if (params.RELEASE) {
            versionToBuild = utils.getReleaseVersion(params.RELEASE_VERSION, env.INITIAL_POM_VERSION)
          } else {
            versionToBuild = readMavenPom().getVersion()
          }
          sh "docker/docker-build.sh ${versionToBuild} ${params.RELEASE}"
        }
      }
    }
  }
  post {
    success {
      echo 'Pipeline executed successfully!'
    }
    failure {
      echo 'Pipeline execution failed!'
    }
  }
}
