/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.olm;

import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.resources.operator.ClusterOperatorConfigurationBuilder;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.utils.specific.OlmUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import static io.strimzi.systemtest.TestConstants.WATCH_ALL_NAMESPACES;
import static io.strimzi.systemtest.TestTags.BRIDGE;
import static io.strimzi.systemtest.TestTags.CONNECT;
import static io.strimzi.systemtest.TestTags.CRUISE_CONTROL;
import static io.strimzi.systemtest.TestTags.MIRROR_MAKER2;
import static io.strimzi.systemtest.TestTags.OLM;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Tag(OLM)
@SuiteDoc(
    description = @Desc("Tests Strimzi deployments managed by OLM when configured to watch all namespaces."),
    labels = {
        @Label(TestDocsLabels.OLM)
    }
)
public class OlmAllNamespaceST extends OlmAbstractST {

    public static final String NAMESPACE = "olm-namespace";

    @Test
    @Order(1)
    @TestDoc(
        description = @Desc("Verifies the deployment of a Kafka cluster using the OLM example when the operator watches all namespaces."),
        steps = {
            @Step(value = "Create a Kafka CR using the OLM example.", expected = "Kafka CR is created in Kubernetes."),
            @Step(value = "Wait for readiness of the Kafka cluster, meaning that the operator (watching all namespaces) manages the Kafka cluster in the designated test namespace.", expected = "Kafka cluster is operational and managed by the operator.")
        },
        labels = {
            @Label(TestDocsLabels.OLM)
        }
    )
    void testDeployExampleKafka() {
        doTestDeployExampleKafka();
    }

    @Test
    @Order(2)
    @TestDoc(
        description = @Desc("Verifies the deployment of a KafkaUser using the OLM example when the operator watches all namespaces."),
        steps = {
            @Step(value = "Deploy Kafka cluster with simple authorization.", expected = "Kafka cluster with simple authz is deployed and ready."),
            @Step(value = "Deploy KafkaUser using the OLM example.", expected = "KafkaUser is deployed and becomes ready."),
            @Step(value = "Wait for readiness of the KafkaUser, meaning that the operator (watching all namespaces) manages the KafkaUser in the designated test namespace.", expected = "KafkaUser is operational and managed by the operator.")
        },
        labels = {
            @Label(TestDocsLabels.OLM)
        }
    )
    void testDeployExampleKafkaUser() {
        doTestDeployExampleKafkaUser();
    }

    @Test
    @Order(3)
    @TestDoc(
        description = @Desc("Verifies the deployment of a KafkaTopic using the OLM example when the operator watches all namespaces."),
        steps = {
            @Step(value = "Deploy KafkaTopic using the OLM example.", expected = "KafkaTopic is deployed and becomes ready."),
            @Step(value = "Wait for readiness of the KafkaTopic, meaning that the operator (watching all namespaces) manages the KafkaTopic in the designated test namespace.", expected = "KafkaTopic is operational and managed by the operator.")
        },
        labels = {
            @Label(TestDocsLabels.OLM)
        }
    )
    void testDeployExampleKafkaTopic() {
        doTestDeployExampleKafkaTopic();
    }

    @Test
    @Order(4)
    @Tag(CONNECT)
    @TestDoc(
        description = @Desc("Verifies the deployment of a KafkaConnect cluster using the OLM example when the operator watches all namespaces."),
        steps = {
            @Step(value = "Create a KafkaConnect CR using the OLM example.", expected = "KafkaConnect CR is created in Kubernetes."),
            @Step(value = "Wait for readiness of the KafkaConnect, meaning that the operator (watching all namespaces) manages the KafkaConnect cluster in the designated test namespace.", expected = "KafkaConnect cluster is operational and managed by the operator.")
        },
        labels = {
            @Label(TestDocsLabels.OLM)
        }
    )
    void testDeployExampleKafkaConnect() {
        doTestDeployExampleKafkaConnect();
    }

    @Test
    @Order(5)
    @Tag(BRIDGE)
    @TestDoc(
        description = @Desc("Verifies the deployment of a KafkaBridge using the OLM example when the operator watches all namespaces."),
        steps = {
            @Step(value = "Deploy KafkaBridge CR using the OLM example.", expected = "KafkaBridge CR is created in Kubernetes."),
            @Step(value = "Wait for readiness of the KafkaBridge, meaning that the operator (watching all namespaces) manages the KafkaBridge in the designated test namespace.", expected = "KafkaBridge is operational and managed by the operator.")
        },
        labels = {
            @Label(TestDocsLabels.OLM)
        }
    )
    void testDeployExampleKafkaBridge() {
        doTestDeployExampleKafkaBridge();
    }

    @Test
    @Order(7)
    @Tag(MIRROR_MAKER2)
    @TestDoc(
        description = @Desc("Verifies the deployment of a KafkaMirrorMaker2 cluster using the OLM example when the operator watches all namespaces."),
        steps = {
            @Step(value = "Deploy KafkaMirrorMaker2 CR using the OLM example.", expected = "KafkaMirrorMaker2 CR is created in Kubernetes."),
            @Step(value = "Wait for readiness of the KafkaMirrorMaker2, meaning that the operator (watching all namespaces) manages the KafkaMirrorMaker2 cluster in the designated test namespace.", expected = "KafkaMirrorMaker2 cluster is operational and managed by the operator.")
        },
        labels = {
            @Label(TestDocsLabels.OLM)
        }
    )
    void testDeployExampleKafkaMirrorMaker2() {
        doTestDeployExampleKafkaMirrorMaker2();
    }

    @Test
    @Order(8)
    @Tag(CRUISE_CONTROL)
    @TestDoc(
        description = @Desc("Verifies the deployment of a KafkaRebalance resource using the OLM example when the operator watches all namespaces."),
        steps = {
            @Step(value = "Deploy KafkaRebalance CR using the OLM example.", expected = "KafkaRebalance CR created in Kubernetes and reaches PendingProposal state."),
            @Step(value = "Wait for readiness of the KafkaRebalance, meaning that the operator (watching all namespaces) manages the KafkaRebalance resource in the designated test namespace.", expected = "KafkaRebalance resource is operational and managed by the operator.")
        },
        labels = {
            @Label(TestDocsLabels.OLM)
        }
    )
    void testDeployExampleKafkaRebalance() {
        doTestDeployExampleKafkaRebalance();
    }

    @BeforeAll
    void setup() {
        SetupClusterOperator
            .getInstance()
            .withCustomConfiguration(new ClusterOperatorConfigurationBuilder()
                .withNamespacesToWatch(WATCH_ALL_NAMESPACES)
                .build()
            )
            .installUsingOlm();

        exampleResources = OlmUtils.getExamplesFromCsv(
            SetupClusterOperator.getInstance().getOperatorNamespace(),
            SetupClusterOperator.getInstance().getOlmClusterOperatorConfiguration().getOlmAppBundlePrefix()
        );

        cluster.setNamespace(Environment.TEST_SUITE_NAMESPACE);
    }
}
