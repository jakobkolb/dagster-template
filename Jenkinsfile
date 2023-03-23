// Jenkinsfile executing in two containers on the same node
pipeline {
    // Assuming python 3.10 and docker / docker compose are available on the main node.
    agent {
        kubernetes {
            defaultContainer 'python'
            yaml """
                apiVersion: v1
                kind: Pod
                metadata:
                  labels:
                    jenkins: build-node
                spec:
                    containers:
                    - name: python
                      image: ghcr.io/rocs-org/python-test-environment:latest
                      command:
                      - cat
                      tty: true
                      env:
                      - name: http_proxy
                        value: http://10.15.156.29:8020
                      - name: https_proxy
                        value: http://10.15.156.29:8020
                      - name: NO_PROXY
                        value: localhost
                    - name: kaniko
                      image: gcr.io/kaniko-project/executor:debug
                      imagePullPolicy: Always
                      command:
                        - cat
                      tty: true
                      env:
                      - name: HTTP_PROXY
                        value: http://10.15.156.29:8020
                      - name: HTTPS_PROXY
                        value: http://10.15.156.29:8020
                      - name: NO_PROXY
                        value: harbor.it32.labor
                    restartPolicy: Never
                """
            }
    }
    stages {
        stage('Setup'){
            steps {
                sh 'poetry --version'
                // Checkout the code
                checkout scm
                // Install dependencies
                sh 'make install'
            }
        }
        stage('Test and lint') {
            steps {
                // Run linting
                sh 'make lint'
                // Run tests
                sh 'make test'
            }
        }
        stage('Build and publish') {
            when {
                branch 'main'
            }
            steps {
                container('kaniko') {
                    withCredentials([usernamePassword(credentialsId: 'harbor', usernameVariable: 'username', passwordVariable: 'password')]) {
                        sh '''
                            # Setup login credentials for harbor. Note, you have to double escape the quotes because of
                            # https://stackoverflow.com/a/56596103
                            export TOKEN=$(echo -n $username:$password | base64)
                            echo "{\\"auths\\":{\\"harbor.it32.labor\\":{\\"username\\":\\"$username\\",\\"password\\":\\"$password\\",\\"auth\\":\\"$TOKEN\\"}}}" > /kaniko/.docker/config.json
                            # Build and publish the image. Note you have to pass the proxy as build args.
                            /kaniko/executor \
                            --dockerfile=docker/Dockerfile \
                            --context=dir://. \
                            --destination=harbor.it32.labor/devops/dagster-poc-it24:latest \
                            --skip-tls-verify-registry=harbor.it32.labor \
                            --build-arg "http_proxy=$HTTP_PROXY" \
                            --build-arg "https_proxy=$HTTPS_PROXY" \
                            --build-arg "HTTP_PROXY=$HTTP_PROXY" \
                            --build-arg "HTTPS_PROXY=$HTTPS_PROXY"
                        '''
                    }
                }
            }
        }
    }
}