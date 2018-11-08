timestamps {
    node() {
        def userId = "facilitador"
        def mainClass = "com.thoughtworks.exercises.batch.Persistence"
        def artifactBucket = "com.thoughtworks.training.de.recife/${userId}/bin"

        stage('Spark Exercises - Checkout') {
            checkout([$class: 'GitSCM', branches: [[name: '*/master']], doGenerateSubmoduleConfigurations: false, extensions: [], submoduleCfg: [], userRemoteConfigs: [[credentialsId: '', url: 'https://github.com/jpaulorio/spark-training.git']]])
        }

        stage('Spark Exercises - Build') {
            echo "User id: ${userId}"

            sh """
        sbt clean assembly
      """
        }

        stage('Spark Exercises - Publish') {
            s3Upload(consoleLogLevel: 'INFO', dontWaitForConcurrentBuildCompletion: false, entries: [[bucket: "${artifactBucket}", excludedFile: '', flatten: true, gzipFiles: false, keepForever: false, managedArtifacts: false, noUploadOnFailure: true, selectedRegion: 'us-east-1', showDirectlyInBrowser: false, sourceFile: '**/target/scala-2.11/*.jar', storageClass: 'STANDARD', uploadFromSlave: false, useServerSideEncryption: false]], pluginFailureResultConstraint: 'FAILURE', profileName: 'AWS DE Trainining Recife', userMetadata: [])
        }

        stage('Spark Exercises - Deploy') {
            sh """
          clusterId=\$(aws emr list-clusters --cluster-states WAITING --query 'Clusters[?Name==`${userId}`].Id' | cut -c6-20 | head -n 2 | tail -n +2 | sed 's/"//')
          echo \$clusterId
          aws emr add-steps --cluster-id \$clusterId --steps Type=Spark,Name="Spark Exercises",ActionOnFailure=CONTINUE,Args=[--class,${mainClass},s3://${artifactBucket}/de-training-0.1-SNAPSHOT.jar,Arg1]
        """
        }
    }
}