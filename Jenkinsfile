timestamps {
  node () {
    def userId = "facilitador"

    stage ('Spark Exercises - Checkout') {
      checkout([$class: 'GitSCM', branches: [[name: '*/master']], doGenerateSubmoduleConfigurations: false, extensions: [], submoduleCfg: [], userRemoteConfigs: [[credentialsId: '', url: 'https://github.com/jpaulorio/spark-training.git']]])
    }

    stage ('Spark Exercises - Build') {
      echo "User id: ${userId}"

      sh """
        sbt assembly
      """
    }

    stage('Spark Exercises - Publish') {
      s3Upload(consoleLogLevel: 'INFO', dontWaitForConcurrentBuildCompletion: false, entries: [[bucket: "com.thoughtworks.training.de.recife/${userId}", excludedFile: '', flatten: true, gzipFiles: false, keepForever: false, managedArtifacts: false, noUploadOnFailure: true, selectedRegion: 'us-east-1', showDirectlyInBrowser: false, sourceFile: '**/target/scala-2.11/*.jar', storageClass: 'STANDARD', uploadFromSlave: false, useServerSideEncryption: false]], pluginFailureResultConstraint: 'FAILURE', profileName: 'AWS DE Trainining Recife', userMetadata: [])
    }

    stage('Spark Exercises - Deploy') {
      def doDeploy = false
      timeout(time: 15, unit: 'SECONDS') {
        doDeploy = input(message: 'Arrocha?', ok: 'Podicrê',
                         parameters: [booleanParam(defaultValue: true,
                         description: 'Vai com fé!',name: 'Arrocha?')])
      }

      if (doDeploy) {
        sh """
          clusterId=$(aws emr list-clusters --cluster-states RUNNING --query 'Clusters[?Name==`${userId}`].Id' | cut -c6-19 | head -n 2 | tail -n +2)
          aws emr add-steps --cluster-id $clusterId --steps Type=CUSTOM_JAR,Name=CustomJAR,ActionOnFailure=CONTINUE,Jar=s3://com.thoughtworks.training.de.recife/${userId}/de-training-0.1-SNAPSHOT.jar,Args=arg1,arg2,arg3
        """
      }
    }
  }
}