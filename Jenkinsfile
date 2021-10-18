buildSuccess = true
Master_Build_ID = "1.1.0-${env.BRANCH_NAME}-${env.BUILD_NUMBER}"
grpId = "com.tfs.dp"
artId = "Orchestrator"
packageType = "jar"
artifactName = "${artId}-${Master_Build_ID}.${packageType}"
archiveLocation = "./target/scala-2.11/Orchestrator-assembly-1.0.jar"

mavenVersion="apache-maven-3.3.3"
nodejsVersion="node-v4.4.6-linux-x64"
grailsVersion="grails-2.5.0"
gradleVersion="gradle-2.3"

node('jenkins-slave04.pool.sv2.247-inc.net')
{
	env.JAVA_HOME = "${env.jdk7_home}"
	sh "${env.JAVA_HOME}/bin/java -version"
	echo "Current branch <${env.BRANCH_NAME}>"
	def workspace = env.WORKSPACE
	
	stage('Preparation') 
	{
		executeCheckout() 
	}
	if(env.CHANGE_ID)
	{
		stage('commit')
		{
			echo "pull request detected"
			buildSuccess = executeBuild()
			echo "buildSuccess = ${buildSuccess}"
			validateBuild(buildSuccess)
		}
		if(currentBuild.result != 'FAILURE')
		{		
			stage('Sonar Analysis') 
			{
				codeCheck()
			}
			
		}
		
	}
	if(!env.CHANGE_ID)
	{
		stage('sanity')
		{
			echo "push detected"
			buildSuccess = executeBuildsanity()
			echo "buildSuccess = ${buildSuccess}"
			validateBuild(buildSuccess)
		}	
		if(currentBuild.result != 'FAILURE')
		{		
			stage('Sonar Analysis') 
			{
				codeCheck()
			}
			
		}
		
		if(currentBuild.result != 'FAILURE')
		{
			stage("Artifacts upload to nexus")
			{
			    sh  '''
				    mavenVersion='''+mavenVersion+'''
				
				REPO_URL=${NEXUS_REPO_URL_DEFAULT}
				REPO_ID=${NEXUS_REPO_ID_DEFAULT}
				GRP_ID='''+grpId+'''
				ART_ID='''+artId+'''
				PACKAGE_TYPE='''+packageType+'''
				ARTIFACT_NAME='''+artifactName+'''
				Master_Build_ID='''+Master_Build_ID+'''
				ZIP_FILE='''+archiveLocation+'''
				/opt/${mavenVersion}/bin/mvn -B deploy:deploy-file -Durl=$REPO_URL -DrepositoryId=$REPO_ID -DgroupId=$GRP_ID -DartifactId=$ART_ID -Dversion=$Master_Build_ID -Dfile=$ZIP_FILE -Dpackaging=$PACKAGE_TYPE -DgeneratePom=true -e
				/opt/${mavenVersion}/bin/mvn -B deploy:deploy-file -Durl=$REPO_URL -DrepositoryId=$REPO_ID -DgroupId=$GRP_ID -DartifactId=$ART_ID -Dversion=latest -Dfile=$ZIP_FILE -Dpackaging=$PACKAGE_TYPE -DgeneratePom=true -e
		
				'''
				
			}
		}
		
		
		
		
	}
}

def boolean executeBuild()
{
	def result = true
	def branchName = env.BRANCH_NAME
	echo "branch = ${branchName} Master_Build_ID=${Master_Build_ID}"
			try 
			{
				sh '''	export JAVA_HOME=${jdk8_home}
						export PATH=${jdk8_home}/bin:$PATH
						mavenVersion='''+mavenVersion+'''
						nodejsVersion='''+nodejsVersion+'''
						grailsVersion='''+grailsVersion+'''
						gradleVersion='''+gradleVersion+'''
						
						export PATH=$PATH:/opt/${mavenVersion}/bin
						export PATH=/opt/${nodejsVersion}/bin:$PATH
						export PATH=/var/tellme/jenkins/tools/sbt/bin:$PATH
						export PATH=/opt/${grailsVersion}/bin:$PATH
						export PATH=/opt/${gradleVersion}/bin:$PATH
						
						BRANCH='''+branchName+'''
					#ADD YOUR BUILD STEPS HERE----------------------------------
					export http_proxy=http://proxy-grp1.lb-priv.sv2.247-inc.net:3128
					export https_proxy=http://proxy-grp1.lb-priv.sv2.247-inc.net:3128
					/var/tellme/opt/sbt/bin/sbt clean compile test assembly
					#-----------------------------------------------------------

				'''
				echo "Build Success...."
				result = true
			} 
			catch(Exception ex) 
			{
				 echo "Build Failed...."
				 echo "ex.toString() - ${ex.toString()}"
				 echo "ex.getMessage() - ${ex.getMessage()}"
				 echo "ex.getStackTrace() - ${ex.getStackTrace()}"
				 result = false
			} 
		
	
	echo "result - ${result}"
	result
}


def codeCheck()
{
	withSonarQubeEnv('SonarServer')
	{				    	
		try 
			{
				
				sh '''	export JAVA_HOME=${jdk8_home}
						export PATH=${jdk8_home}/bin:$PATH
						mavenVersion='''+mavenVersion+'''
						nodejsVersion='''+nodejsVersion+'''
						grailsVersion='''+grailsVersion+'''
						gradleVersion='''+gradleVersion+'''
						
						export PATH=$PATH:/opt/${mavenVersion}/bin
						export PATH=/opt/${nodejsVersion}/bin:$PATH
						export PATH=/var/tellme/jenkins/tools/sbt/bin:$PATH
						export PATH=/opt/${grailsVersion}/bin:$PATH
						export PATH=/opt/${gradleVersion}/bin:$PATH
			
		
			
					#ADD YOUR BUILD STEPS HERE----------------------------------
					export http_proxy=http://proxy-grp1.lb-priv.sv2.247-inc.net:3128
					export https_proxy=http://proxy-grp1.lb-priv.sv2.247-inc.net:3128
					/var/tellme/opt/sbt/bin/sbt coverage test coverageReport coverageAggregate
					/var/tellme/opt/sbt/bin/sbt sonarScan
					#-----------------------------------------------------------
				'''
			
				step([$class: 'ScoveragePublisher', reportDir: 'target/scala-2.11/scoverage-report', reportFile: 'scoverage.xml'])
					
				echo "Build Success...."
				result = true
				} 
		catch(Exception ex) 
		{
				 echo "Build Failed...."
				 echo "ex.toString() - ${ex.toString()}"
				 echo "ex.getMessage() - ${ex.getMessage()}"
				 echo "ex.getStackTrace() - ${ex.getStackTrace()}"
				 result = false
		} 
	}	    
	//validateBuild(result)	
			stage("Quality Gate") 
			{
				//Following line handles sonarserver's slow response
				timeout(time: 1, unit: 'HOURS')
				{
					def qg = waitForQualityGate()
					if (qg.status =='ERROR')
					{
						error "Pipeline aborted due to quality gate failure: ${qg.status}"
						currentBuild.result ='FAILURE'
					}
				}
			}
}





def executeBuildsanity()
{
	def result = true
	def branchName = env.BRANCH_NAME
	echo "branch = ${branchName}"
	try 
	{
		sh '''	export JAVA_HOME=${jdk8_home}
				export PATH=${jdk8_home}/bin:$PATH
				mavenVersion='''+mavenVersion+'''
				nodejsVersion='''+nodejsVersion+'''
				grailsVersion='''+grailsVersion+'''
				gradleVersion='''+gradleVersion+'''
				
				export PATH=$PATH:/opt/${mavenVersion}/bin
				export PATH=/opt/${nodejsVersion}/bin:$PATH
				export PATH=/var/tellme/jenkins/tools/sbt/bin:$PATH
				export PATH=/opt/${grailsVersion}/bin:$PATH
				export PATH=/opt/${gradleVersion}/bin:$PATH
				
				BRANCH='''+branchName+'''

				#ADD YOUR BUILD STEPS HERE----------------------------------
				export http_proxy=http://proxy-grp1.lb-priv.sv2.247-inc.net:3128
				export https_proxy=http://proxy-grp1.lb-priv.sv2.247-inc.net:3128
				/var/tellme/opt/sbt/bin/sbt clean compile test assembly
				#-----------------------------------------------------------
		'''
		/*		REPO_URL=${NEXUS_REPO_URL_DEFAULT}
				REPO_ID=${NEXUS_REPO_ID_DEFAULT}
				GRP_ID='''+grpId+'''
				ART_ID='''+artId+'''
				PACKAGE_TYPE='''+packageType+'''
				ARTIFACT_NAME='''+artifactName+'''
				Master_Build_ID='''+Master_Build_ID+'''
				ZIP_FILE='''+archiveLocation+'''
				/opt/${mavenVersion}/bin/mvn -B deploy:deploy-file -Durl=$REPO_URL -DrepositoryId=$REPO_ID -DgroupId=$GRP_ID -DartifactId=$ART_ID -Dversion=$Master_Build_ID -Dfile=$ZIP_FILE -Dpackaging=$PACKAGE_TYPE -DgeneratePom=true -e
				/opt/${mavenVersion}/bin/mvn -B deploy:deploy-file -Durl=$REPO_URL -DrepositoryId=$REPO_ID -DgroupId=$GRP_ID -DartifactId=$ART_ID -Dversion=latest -Dfile=$ZIP_FILE -Dpackaging=$PACKAGE_TYPE -DgeneratePom=true -e
		
		*/
			
		
		echo "Build Success...."
		result = true
	}
	catch(Exception ex) 
	{
		 echo "Build Failed...."
		 echo "ex.toString() - ${ex.toString()}"
		 echo "ex.getMessage() - ${ex.getMessage()}"
		 echo "ex.getStackTrace() - ${ex.getStackTrace()}"
		 result = false
	} 
	result 
}
def executeCheckout()
{
  //Get some code from a GitHub repository
  checkout scm
}

def validateBuild(def buildStatus)
{
	if (buildStatus) 
	{
		  currentBuild.result = 'SUCCESS'
	}
	else
	{
		currentBuild.result = 'FAILURE'
		 error "build failed!"
	}
	
}
