# Workflow Service

![Service Builder API Test and Build](https://github.com/DigitalPatterns/workflow-service/workflows/Service%20Builder%20API%20Test%20and%20Build/badge.svg)


Integrated Camunda engine with Cockpit.


#### Bootstrap configuration

The following environment variables are required to load properties from AWS secrets manager

* AWS_SECRETS_MANAGER_ENABLED
* AWS_REGION
* AWS_ACCESS_KEY
* AWS_SECRET_KEY
* SPRING_PROFILES_ACTIVE


#### Application configuration

The following properties need to be configured in AWS secrets manager

* database.driver-class-name
* database.password
* database.username
* auth.url
* auth.clientId
* auth.clientSecret
* auth.realm
* engine.webhook.url
* aws.s3.formData
* aws.s3.pdfs
* formApi.url
* gov.notify.api.key
