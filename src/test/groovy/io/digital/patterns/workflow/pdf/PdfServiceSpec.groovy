package io.digital.patterns.workflow.pdf

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import com.amazonaws.services.simpleemail.AmazonSimpleEmailService
import com.github.tomakehurst.wiremock.client.WireMock
import com.github.tomakehurst.wiremock.junit.WireMockRule
import com.github.tomjankes.wiremock.WireMockGroovy
import org.camunda.bpm.engine.runtime.ProcessInstance
import org.camunda.bpm.engine.test.ProcessEngineRule
import org.camunda.bpm.engine.test.mock.Mocks
import org.camunda.bpm.model.bpmn.Bpmn
import org.camunda.bpm.model.bpmn.BpmnModelInstance
import org.camunda.spin.DataFormats
import org.junit.ClassRule
import org.springframework.core.env.Environment
import org.springframework.core.env.StandardEnvironment
import org.springframework.core.io.ClassPathResource
import org.springframework.web.client.RestTemplate
import org.testcontainers.containers.localstack.LocalStackContainer
import spock.lang.Shared
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse
import static com.github.tomakehurst.wiremock.http.Response.response
import static org.camunda.bpm.engine.test.assertions.bpmn.BpmnAwareTests.assertThat
import static org.camunda.bpm.engine.test.assertions.bpmn.BpmnAwareTests.repositoryService
import static org.camunda.bpm.engine.test.assertions.bpmn.BpmnAwareTests.runtimeService
import static org.camunda.spin.Spin.S

class PdfServiceSpec extends Specification {

    def static wmPort = 8000


    @ClassRule
    @Shared
    ProcessEngineRule engineRule = new ProcessEngineRule()

    @ClassRule
    @Shared
    WireMockRule wireMockRule = new WireMockRule(wmPort)

    public wireMockStub = new WireMockGroovy(wmPort)

    @Shared
    static LocalStackContainer localstack =
            new LocalStackContainer().withServices(LocalStackContainer.Service.S3)


    AmazonS3 amazonS3

    PdfService pdfService
    AmazonSimpleEmailService amazonSimpleEmailService
    Environment environment
    RestTemplate restTemplate

    def setupSpec() {
        System.setProperty("aws.bucket-name-prefix", "test")
        System.setProperty('form-api.url', "http://localhost:${wmPort}")
        localstack.start()
    }

    def cleanupSpec() {
        localstack.stop()
    }


    def setup() {
        restTemplate = new RestTemplate()
        environment = new StandardEnvironment()

        amazonSimpleEmailService = Mock()
        final BasicAWSCredentials credentials = new BasicAWSCredentials('accessKey', 'secretAccessKey')

        amazonS3 = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withEndpointConfiguration(localstack.getEndpointConfiguration(LocalStackContainer.Service.S3))
                .enablePathStyleAccess()
                .build()

        pdfService = new PdfService(
                amazonS3,
                amazonSimpleEmailService,
                engineRule.runtimeService,
                environment,
                restTemplate
        )
        Mocks.register('pdfService', pdfService)
        Mocks.register('environment', environment)

    }

    def 'can request pdf generation'() {
        given: 'initial data set up'
        amazonS3.createBucket("test-test")
        amazonS3.putObject(new PutObjectRequest("test-test", "TEST-20200120-000/aForm/xx@x.com-20200128T083155.json",
                new ClassPathResource("data.json").getInputStream(), new ObjectMetadata()))

        and: 'pdf server expectation is set up'
        wireMockStub.stub {
            request {
                method 'POST'
                url '/pdf'
            }
            response {
                status: 200
                headers {
                    "Content-Type" "application/json"
                }
            }
        }

        and: 'a process with service task is created'
        BpmnModelInstance modelInstance = Bpmn.createExecutableProcess("pdf")
                .executable()
                .startEvent()
                .camundaFormKey('sampleForm')
                .serviceTask('generatePdf')
                .name('Generate PDF')
                .camundaExpression('${pdfService.requestPdfGeneration(form, businessKey, product, execution.getProcessInstanceId())}')
                .camundaInputParameter('form', '${exampleForm.prop(\'form\')}')
                .camundaInputParameter('product', 'test')
                .camundaInputParameter('businessKey', '${exampleForm.prop(\'businessKey\').stringValue()}')
                .endEvent()
                .done()

        repositoryService().createDeployment().addModelInstance("pdf.bpmn", modelInstance).deploy()

        when: 'a request to initiate pdf has been submitted'
        ProcessInstance instance = runtimeService()
        .createProcessInstanceByKey('pdf')
        .businessKey('TEST-20200120-000')
        .setVariables(['exampleForm' : S('''{
            "test" : "test",
            "businessKey": "TEST-20200120-000",
            "form" : {
               "name" : "aForm",
               "submittedBy": "xx@x.com",
               "submissionDate": "2020-01-28T08:31:55.297Z",
               "formVersionId": "formVersionId"
            }
        }''', DataFormats.JSON_DATAFORMAT_NAME)]).execute()


        then: 'pdf request was successful'
        assertThat(instance).isEnded()

    }

    def 'can raise an incident if s3 fails'() {
        def conditions = new PollingConditions(timeout: 10, initialDelay: 1.5, factor: 1.25)
        given: 'initial data set up'
        amazonS3.createBucket("test-test")
        amazonS3.putObject(new PutObjectRequest("test-test", "TEST-20200120-000/aForm/xx@x.com-20200128T083155.json",
                new ClassPathResource("data.json").getInputStream(), new ObjectMetadata()))

        and: 'a process with service task is created'
        BpmnModelInstance modelInstance = Bpmn.createExecutableProcess("pdfFailure")
                .executable()
                .startEvent()
                .camundaFormKey('sampleForm')
                .serviceTask('generatePdf')
                .camundaAsyncBefore()
                .camundaAsyncAfter()
                .name('Generate PDF')
                .camundaExpression('${pdfService.requestPdfGeneration(form, businessKey, product, execution.getProcessInstanceId())}')
                .camundaInputParameter('form', '${exampleForm.prop(\'form\')}')
                .camundaInputParameter('product', 'test')
                .camundaInputParameter('businessKey', '${exampleForm.prop(\'businessKey\').stringValue()}')
                .endEvent()
                .done()

        repositoryService().createDeployment().addModelInstance("pdfFailure.bpmn", modelInstance).deploy()

        when: 'pdf fails'
        wireMockRule.stubFor(WireMock.post(WireMock.urlMatching("/pdf")).willReturn(
                aResponse().withStatus(500))
        )
        ProcessInstance instance = runtimeService()
                .createProcessInstanceByKey('pdfFailure')
                .businessKey('TEST-20200120-000')
                .setVariables(['exampleForm' : S('''{
            "test" : "test",
            "businessKey": "TEST-20200120-000",
            "form" : {
               "name" : "aForm",
               "submittedBy": "xx@x.com",
               "submissionDate": "2020-01-28T08:31:55.297Z",
               "formVersionId": "formVersionId"
            }
        }''', DataFormats.JSON_DATAFORMAT_NAME)]).execute()

        then: 'incident is created'
        assertThat(instance).isActive()
        conditions.eventually {
            def incidents = runtimeService().createIncidentQuery()
                    .processInstanceId(instance.id).list()

            incidents.size() != 0
        }
    }
}
