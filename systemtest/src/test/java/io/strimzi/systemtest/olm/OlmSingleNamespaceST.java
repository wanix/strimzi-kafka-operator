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

import static io.strimzi.systemtest.TestTags.BRIDGE;
import static io.strimzi.systemtest.TestTags.CONNECT;
import static io.strimzi.systemtest.TestTags.CRUISE_CONTROL;
import static io.strimzi.systemtest.TestTags.MIRROR_MAKER2;
import static io.strimzi.systemtest.TestTags.OLM;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Tag(OLM)
@SuiteDoc(
    description = @Desc("Tests Strimzi deployments managed by OLM when configured to watch a single, specific namespace."),
    labels = {
        @Label(TestDocsLabels.OLM)
    }
)
public class OlmSingleNamespaceST extends OlmAbstractST {

    public static final String NAMESPACE = "olm-namespace";

    @Test
    @Order(1)
    @TestDoc(
        description = @Desc("Verifies the deployment of a Kafka cluster using the OLM example in a single-namespace watch configuration."),
        steps = {
            @Step(value = "Deploy Kafka CR using the OLM example in the designated single namespace.", expected = "Kafka CR is created in Kubernetes within the watched namespace.."),
            @Step(value = "Verify that the Strimzi operator (watching a single namespace) correctly deploys and manages the resource in that same namespace.", expected = "The resource is operational and managed by the operator within its watched namespace.")
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
        description = @Desc("Verifies the deployment of a KafkaUser using the OLM example in a single-namespace watch configuration."),
        steps = {
            @Step(value = "Deploy Kafka CR with simple authorization.", expected = "Kafka CR is created with simple authz."),
            @Step(value = "Deploy KafkaUser using the OLM example in the designated single namespace.", expected = "KafkaUser is deployed and becomes ready within the watched namespace."),
            @Step(value = "Verify that the Strimzi operator (watching a single namespace) correctly deploys and manages the resource in that same namespace.", expected = "The resource is operational and managed by the operator within its watched namespace.")
        },
        labels = {
            @Label(TestDocsLabels.OLM),
        }
    )
    void testDeployExampleKafkaUser() {
        doTestDeployExampleKafkaUser();
    }

    @Test
    @Order(3)
    @TestDoc(
        description = @Desc("Verifies the deployment of a KafkaTopic using the OLM example in a single-namespace watch configuration."),
        steps = {
            @Step(value = "Deploy KafkaTopic CR using the OLM example in the designated single namespace.", expected = "KafkaTopic CR is created in Kubernetes within the watched namespace."),
            @Step(value = "Verify that the Strimzi operator (watching a single namespace) correctly deploys and manages the resource in that same namespace.", expected = "The resource is operational and managed by the operator within its watched namespace.")
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
        description = @Desc("Verifies the deployment of a KafkaConnect cluster using the OLM example in a single-namespace watch configuration."),
        steps = {
            @Step(value = "Deploy KafkaConnect CR using the OLM example in the designated single namespace.", expected = "KafkaConnect CR is created in Kubernetes within the watched namespace.."),
            @Step(value = "Verify that the Strimzi operator (watching a single namespace) correctly deploys and manages the resource in that same namespace.", expected = "The resource is operational and managed by the operator within its watched namespace.")
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
        description = @Desc("Verifies the deployment of a KafkaBridge using the OLM example in a single-namespace watch configuration."),
        steps = {
            @Step(value = "Deploy KafkaBridge CR using the OLM example in the designated single namespace.", expected = "KafkaBridge CR is created in Kubernetes within the watched namespace.."),
            @Step(value = "Verify that the Strimzi operator (watching a single namespace) correctly deploys and manages the resource in that same namespace.", expected = "The resource is operational and managed by the operator within its watched namespace.")
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
        description = @Desc("Verifies the deployment of a KafkaMirrorMaker2 cluster using the OLM example in a single-namespace watch configuration."),
        steps = {
            @Step(value = "Deploy KafkaMirrorMaker2 CR using the OLM example in the designated single namespace.", expected = "KafkaMirrorMaker2 CR is created in Kubernetes within the watched namespace."),
            @Step(value = "Verify that the Strimzi operator (watching a single namespace) correctly deploys and manages the resource in that same namespace.", expected = "The resource is operational and managed by the operator within its watched namespace.")
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
        description = @Desc("Verifies the deployment of a KafkaRebalance resource using the OLM example in a single-namespace watch configuration."),
        steps = {
            @Step(value = "Deploy KafkaRebalance CR using the OLM example in the designated single namespace.", expected = "KafkaRebalance CR is created in Kubernetes and reaches PendingProposal state within the watched namespace."),
            @Step(value = "Verify that the Strimzi operator (watching a single namespace) correctly deploys and manages the resource in that same namespace.", expected = "The resource is operational and managed by the operator within its watched namespace.")
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
                .withNamespaceName(NAMESPACE)
                .withNamespacesToWatch(NAMESPACE)
                .build()
            )
            .installUsingOlm();

        exampleResources = OlmUtils.getExamplesFromCsv(
            SetupClusterOperator.getInstance().getOperatorNamespace(),
            SetupClusterOperator.getInstance().getOlmClusterOperatorConfiguration().getOlmAppBundlePrefix()
        );

        cluster.setNamespace(NAMESPACE);
    }
}
