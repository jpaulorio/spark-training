timestamps {

node () {

	stage ('Spark Exercises - Checkout') {
 	 checkout([$class: 'GitSCM', branches: [[name: '*/master']], doGenerateSubmoduleConfigurations: false, extensions: [], submoduleCfg: [], userRemoteConfigs: [[credentialsId: '', url: 'https://github.com/jpaulorio/spark-training.git']]])
	}
	stage ('Spark Exercises - Build') {

    def userId = getUserId()

    echo "User id: ${userId}"

    sh """
    sbt assembly
     """
        }
        stage('Spark Exercises - Publish') {
        s3Upload(consoleLogLevel: 'INFO', dontWaitForConcurrentBuildCompletion: false, entries: [[bucket: "com.thoughtworks.training.de.recife/${userId}", excludedFile: '', flatten: true, gzipFiles: false, keepForever: false, managedArtifacts: false, noUploadOnFailure: true, selectedRegion: 'us-east-1', showDirectlyInBrowser: false, sourceFile: '**/target/scala-2.11/*.jar', storageClass: 'STANDARD', uploadFromSlave: false, useServerSideEncryption: false]], pluginFailureResultConstraint: 'FAILURE', profileName: 'AWS DE Trainining Recife', userMetadata: [])
        }
    }
}

@NonCPS
def getUserId() {
   def alljob = env.JOB_NAME.tokenize('/') as String[]
   def proj_name = alljob[0]
   def branch_name = alljob[1]

   echo "Running ${proj_name}/${branch_name}..."

   def job = Jenkins.getInstance().getItemByFullName("${proj_name}/${branch_name}", Job.class)
   def build = job.getBuildByNumber(env.BUILD_ID as int)
   def userId = build.getCause(Cause.UserIdCause).getUserId()
}