/*
 *   Copyright Flux Contributors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.danielgmyers.flux.clients.sfn;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.danielgmyers.flux.RemoteWorkflowExecutor;
import com.danielgmyers.flux.WorkflowStatusChecker;
import com.danielgmyers.flux.clients.sfn.asl.AslGenerator;
import com.danielgmyers.flux.clients.sfn.asl.AslStateMachine;
import com.danielgmyers.flux.clients.sfn.poller.ActivityTaskPoller;
import com.danielgmyers.flux.clients.sfn.step.SfnStepInputAccessor;
import com.danielgmyers.flux.clients.sfn.util.SfnArnFormatter;
import com.danielgmyers.flux.ex.FluxException;
import com.danielgmyers.flux.ex.WorkflowExecutionException;
import com.danielgmyers.flux.poller.TaskNaming;
import com.danielgmyers.flux.step.PartitionedWorkflowStep;
import com.danielgmyers.flux.step.StepAttributes;
import com.danielgmyers.flux.step.WorkflowStep;
import com.danielgmyers.flux.threads.BlockOnSubmissionThreadPoolExecutor;
import com.danielgmyers.flux.threads.ThreadUtils;
import com.danielgmyers.flux.util.AwsRetryUtils;
import com.danielgmyers.flux.wf.Periodic;
import com.danielgmyers.flux.wf.Workflow;
import com.danielgmyers.flux.wf.graph.WorkflowGraphNode;
import com.danielgmyers.metrics.MetricRecorder;
import com.danielgmyers.metrics.MetricRecorderFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;
import software.amazon.awssdk.core.retry.conditions.RetryCondition;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sfn.SfnClient;
import software.amazon.awssdk.services.sfn.SfnClientBuilder;
import software.amazon.awssdk.services.sfn.model.ActivityListItem;
import software.amazon.awssdk.services.sfn.model.CreateActivityRequest;
import software.amazon.awssdk.services.sfn.model.CreateStateMachineRequest;
import software.amazon.awssdk.services.sfn.model.CreateStateMachineResponse;
import software.amazon.awssdk.services.sfn.model.DescribeStateMachineRequest;
import software.amazon.awssdk.services.sfn.model.DescribeStateMachineResponse;
import software.amazon.awssdk.services.sfn.model.ExecutionAlreadyExistsException;
import software.amazon.awssdk.services.sfn.model.ListActivitiesRequest;
import software.amazon.awssdk.services.sfn.model.StartExecutionRequest;
import software.amazon.awssdk.services.sfn.model.StartExecutionResponse;
import software.amazon.awssdk.services.sfn.model.StateMachineDoesNotExistException;
import software.amazon.awssdk.services.sfn.model.StateMachineType;
import software.amazon.awssdk.services.sfn.model.UpdateStateMachineRequest;

/**
 * The primary class through which the Flux library is used at runtime.
 */
public class FluxCapacitorImpl implements SfnFluxCapacitor {

    private static final Logger log = LoggerFactory.getLogger(FluxCapacitorImpl.class);

    private static final String LIST_ACTIVITIES_METRIC_PREFIX = "Flux.ListActivities";
    private static final String CREATE_ACTIVITY_METRIC_PREFIX = "Flux.CreateActivity";
    private static final String DESCRIBE_STATE_MACHINE_METRIC_PREFIX = "Flux.DescribeStateMachine";
    private static final String CREATE_STATE_MACHINE_METRIC_PREFIX = "Flux.CreateStateMachine";
    private static final String UPDATE_STATE_MACHINE_METRIC_PREFIX = "Flux.UpdateStateMachine";
    private static final String START_EXECUTION_METRIC_PREFIX = "Flux.StartExecution";

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    private final SfnClient sfn;
    private final FluxCapacitorConfig config;
    private final MetricRecorderFactory metricsFactory;
    private final Clock clock;

    private final Map<Class<? extends Workflow>, Workflow> workflowsByClass;
    private final Map<String, WorkflowStep> activitiesByName;
    private final Map<String, Workflow> workflowsByActivityName;

    private Map<String, ScheduledExecutorService> activityTaskPollerThreadsPerActivity;
    private Map<String, BlockOnSubmissionThreadPoolExecutor> workerThreadsPerTaskList;
    private ScheduledExecutorService periodicWorkflowScheduler;

    // The default throttling refill rate for the CreateActivity APIs is 1 per second,
    // so there's no sense retrying more frequently than that by default.
    private static final long REGISTRATION_MAX_RETRY_ATTEMPTS = 5;
    private static final Duration REGISTRATION_MIN_RETRY_DELAY = Duration.ofSeconds(1);
    private static final Duration REGISTRATION_MAX_RETRY_DELAY = Duration.ofSeconds(5);

    /**
     * Initializes a FluxCapacitor object. Package-private for unit test use.
     *
     * @param sfn    - The client that should be used for all Step Functions API calls
     * @param config - Config data used to configure FluxCapacitor behavior.
     */
    FluxCapacitorImpl(MetricRecorderFactory metricsFactory, SfnClient sfn, FluxCapacitorConfig config, Clock clock) {
        this.metricsFactory = metricsFactory;
        this.sfn = sfn;
        this.config = config;
        this.clock = clock;

        this.workflowsByClass = new HashMap<>();
        this.activitiesByName = new HashMap<>();
        this.workflowsByActivityName = new HashMap<>();
    }

    // package-private for test access.
    Map<Class<? extends Workflow>, Workflow> getWorkflowsByClass() {
        return workflowsByClass;
    }

    // package-private for test access.
    Map<String, WorkflowStep> getActivitiesByName() {
        return activitiesByName;
    }

    // package-private for test access.
    Map<String, Workflow> getWorkflowsByActivityName() {
        return workflowsByActivityName;
    }

    /**
     * Creates a FluxCapacitor object and does various bits of setup.
     * Intentionally package-private, only the Factory should be using the constructor.
     *
     * @param metricsFactory - A factory that produces MetricRecorder objects for emitting workflow metrics.
     * @param credentials    - A provider for the AWS credentials that should be used to call AWS APIs
     * @param config         - Configuration data for FluxCapacitor to use to configure itself
     */
    static SfnFluxCapacitor create(MetricRecorderFactory metricsFactory, AwsCredentialsProvider credentials,
                                FluxCapacitorConfig config) {
        // We do our own retry/backoff logic so we can get decent metrics, so here we disable the SDK's defaults.
        RetryPolicy retryPolicy = RetryPolicy.builder()
                .retryCondition(RetryCondition.none())
                .numRetries(0)
                .backoffStrategy(BackoffStrategy.none())
                .throttlingBackoffStrategy(BackoffStrategy.none())
                .build();

        // If an override config was provided, use it, and only use the above RetryPolicy
        // if the provided overrideConfig did not include its own RetryPolicy.
        ClientOverrideConfiguration overrideConfig = config.getClientOverrideConfiguration();
        if (overrideConfig == null) {
            overrideConfig = ClientOverrideConfiguration.builder()
                    .retryPolicy(retryPolicy)
                    .build();
        } else if (overrideConfig.retryPolicy().isEmpty()) {
            overrideConfig = overrideConfig.toBuilder().retryPolicy(retryPolicy).build();
        }

        SfnClientBuilder builder = SfnClient.builder()
                .credentialsProvider(credentials)
                .region(Region.of(config.getAwsRegion()))
                .overrideConfiguration(overrideConfig);

        // if the user specified a custom endpoint, we'll use it;
        // otherwise the SDK will figure it out based on the region name.
        if (config.getSfnEndpoint() != null && !"".equals(config.getSfnEndpoint())) {
            builder.endpointOverride(URI.create(config.getSfnEndpoint()));
        }

        return new FluxCapacitorImpl(metricsFactory, builder.build(), config, Clock.systemUTC());
    }

    @Override
    public void initialize(List<Workflow> workflows) {
        if (workflows == null || workflows.isEmpty()) {
            throw new IllegalArgumentException("The specified workflow list must not be empty.");
        } else if (!workflowsByClass.isEmpty()) {
            throw new IllegalArgumentException("Flux is already initialized.");
        }

        populateMaps(workflows);
        if (config.isRegisterWorkflowsOnStartup()) {
            registerActivities();
            registerWorkflows();
        } else {
            log.info("registerWorkflowsOnStartup is false; skipping CreateActivity / CreateStateMachine. "
                     + "The workflows' state machines and activities are expected to be managed externally "
                     + "(e.g. via CDK using AslGenerator output).");
        }
        initializePollers();
        startPeriodicWorkflows();
    }

    @Override
    public WorkflowStatusChecker executeWorkflow(Class<? extends Workflow> workflowType, String workflowId,
                                                 Map<String, Object> workflowInput) {
        return executeWorkflow(workflowType, workflowId, workflowInput, Collections.emptySet());
    }

    @Override
    public WorkflowStatusChecker executeWorkflow(Class<? extends Workflow> workflowType, String workflowId,
                                                 Map<String, Object> workflowInput, Set<String> executionTags) {
        if (workflowsByClass.isEmpty()) {
            throw new WorkflowExecutionException("Flux has not yet been initialized; "
                                                 + "executeWorkflow() may only be called after initialize().");
        }
        if (!workflowsByClass.containsKey(workflowType)) {
            throw new WorkflowExecutionException("Cannot execute a workflow that was not provided to Flux at "
                                                 + "initialization: " + workflowType.getSimpleName());
        }
        if (executionTags != null && !executionTags.isEmpty()) {
            log.warn("Step Functions does not support workflow execution tags. Ignoring the provided tags: {} "
                     + "for workflow {}", executionTags, workflowId);
        }
        IdentifierValidation.validateWorkflowExecutionId(workflowId);

        Workflow workflow = workflowsByClass.get(workflowType);

        StartExecutionRequest request;
        try {
            request = buildStartWorkflowRequest(workflow, config.getAwsRegion(), config.getAwsAccountId(),
                                                workflowId, workflowInput, clock);
        } catch (JsonProcessingException e) {
            throw new WorkflowExecutionException("Unable to serialize workflow input as JSON.", e);
        }

        try (MetricRecorder metrics = metricsFactory.newMetricRecorder(START_EXECUTION_METRIC_PREFIX)) {
            try {
                StartExecutionResponse response = AwsRetryUtils.executeWithInlineBackoff(
                        () -> sfn.startExecution(request),
                        REGISTRATION_MAX_RETRY_ATTEMPTS,
                        REGISTRATION_MIN_RETRY_DELAY, REGISTRATION_MAX_RETRY_DELAY,
                        metrics, START_EXECUTION_METRIC_PREFIX);
                log.debug("Started workflow {} with id {}: execution arn={}",
                          workflowType.getSimpleName(), workflowId, response.executionArn());
                return new WorkflowStatusCheckerImpl(clock, sfn, response.executionArn());
            } catch (ExecutionAlreadyExistsException e) {
                // SFN execution names are unique within a 90-day window. Flux's contract is that callers can
                // safely retry executeWorkflow with the same workflowId and get back a status checker for the
                // existing execution; we honor that here by reconstructing the execution ARN.
                String executionArn = SfnArnFormatter.executionArn(config.getAwsRegion(),
                                                                    config.getAwsAccountId(),
                                                                    workflowType, workflowId);
                log.debug("Workflow {} with id {} is already running; returning a checker for arn {}",
                          workflowType.getSimpleName(), workflowId, executionArn);
                return new WorkflowStatusCheckerImpl(clock, sfn, executionArn);
            } catch (RuntimeException e) {
                throw new WorkflowExecutionException(
                        "Failed to start workflow " + workflowType.getSimpleName() + " with id " + workflowId, e);
            }
        }
    }

    @Override
    public void writeAslDefinition(Workflow workflow, String awsRegion, String awsAccountId,
                                   OutputStream out) throws IOException {
        AslExporter.writeAslDefinition(workflow, awsRegion, awsAccountId, out);
    }

    @Override
    public List<String> listExpectedActivityNames(Workflow workflow) {
        return AslExporter.listExpectedActivityNames(workflow);
    }

    @Override
    public RemoteWorkflowExecutor getRemoteWorkflowExecutor(String endpointId) {
        // For the regular FluxCapacitor SfnClient, we disabled all the retry logic
        // since we do our own for metrics purposes. However, the remote client is not used
        // for much, and we don't bother emitting metrics for it, so the defaults are fine.

        Function<String, RemoteSfnClientConfig> remoteConfigProvider = config.getRemoteSfnClientConfigProvider();
        if (remoteConfigProvider == null) {
            throw new IllegalStateException("Cannot create a remote workflow executor without a remote client config provider.");
        }
        RemoteSfnClientConfig remoteConfig = remoteConfigProvider.apply(endpointId);
        if (remoteConfig == null) {
            throw new IllegalStateException("Cannot create a remote workflow executor without any remote client config.");
        }

        AwsCredentialsProvider credentials = remoteConfig.getCredentials();
        if (credentials == null) {
            credentials = DefaultCredentialsProvider.create();
        }

        ClientOverrideConfiguration overrideConfig = remoteConfig.getClientOverrideConfiguration();
        if (overrideConfig == null) {
            overrideConfig = ClientOverrideConfiguration.builder().build();
        }

        SfnClientBuilder customSfn = SfnClient.builder().credentialsProvider(credentials)
                .region(Region.of(remoteConfig.getAwsRegion()))
                .overrideConfiguration(overrideConfig);
        if (remoteConfig.getSfnEndpoint() != null) {
            customSfn.endpointOverride(URI.create(remoteConfig.getSfnEndpoint()));
        }
        return new RemoteWorkflowExecutorImpl(clock, metricsFactory, workflowsByClass, customSfn.build(), config, remoteConfig);
    }

    @Override
    public void shutdown() {
        for (ExecutorService s : activityTaskPollerThreadsPerActivity.values()) {
            s.shutdown();
        }
        for (ExecutorService s : workerThreadsPerTaskList.values()) {
            s.shutdown();
        }
        periodicWorkflowScheduler.shutdown();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long timeoutMillis = TimeUnit.MILLISECONDS.convert(timeout, unit);
        timeoutMillis = awaitTerminationAndReturnRemainingMillis(timeoutMillis, activityTaskPollerThreadsPerActivity.values());
        if (timeoutMillis < 0) {
            return false;
        }
        timeoutMillis = awaitTerminationAndReturnRemainingMillis(timeoutMillis, workerThreadsPerTaskList.values());
        if (timeoutMillis < 0) {
            return false;
        }
        return periodicWorkflowScheduler.awaitTermination(timeoutMillis, TimeUnit.MILLISECONDS);
    }

    private long awaitTerminationAndReturnRemainingMillis(long timeoutMillis,
                                                          Collection<? extends ExecutorService> executors)
            throws InterruptedException {
        Duration remaining = Duration.ofMillis(timeoutMillis);
        for (ExecutorService s : executors) {
            if (remaining.isNegative()) {
                return remaining.toMillis();
            }
            Instant start = Instant.now();
            if (!s.awaitTermination(remaining.toMillis(), TimeUnit.MILLISECONDS)) {
                return -1;
            }
            remaining = remaining.minus(Duration.between(start, Instant.now()));
        }
        return remaining.toMillis();
    }

    // Package-private for RemoteWorkflowExecutorImpl and unit test access
    static StartExecutionRequest buildStartWorkflowRequest(Workflow workflow, String awsRegion, String awsAccountId,
                                                           String executionId, Map<String, Object> input,
                                                           Clock clock)
            throws JsonProcessingException {

        SfnStepInputAccessor accessor = new SfnStepInputAccessor(null);
        if (input != null) {
            accessor.addAttributes(input);
        }

        return StartExecutionRequest.builder()
                .input(accessor.toJson())
                .name(executionId)
                .stateMachineArn(SfnArnFormatter.workflowArn(awsRegion, awsAccountId, workflow.getClass()))
                .build();
    }

    // package-private for testing
    void populateMaps(List<Workflow> workflows) {
        Set<String> workflowNames = new HashSet<>();
        for (Workflow workflow : workflows) {
            IdentifierValidation.validateWorkflowName(workflow.getClass());

            String workflowName = TaskNaming.workflowName(workflow.getClass());
            if (workflowNames.contains(workflowName)) {
                String message = "Received more than one Workflow object with the same class name: " + workflowName;
                log.error(message);
                throw new FluxException(message);
            }
            workflowNames.add(workflowName);
            workflowsByClass.put(workflow.getClass(), workflow);

            for (Map.Entry<Class<? extends WorkflowStep>, WorkflowGraphNode> entry : workflow.getGraph().getNodes().entrySet()) {
                IdentifierValidation.validateStepName(entry.getKey());

                WorkflowStep step = entry.getValue().getStep();
                String activityName = buildSfnActivityName(workflow.getClass(), step.getClass());
                if (activitiesByName.containsKey(activityName)) {
                    String message = "Workflow " + workflowName + " has two steps with the same name: "
                            + step.getClass().getSimpleName();
                    log.error(message);
                    throw new FluxException(message);
                }
                activitiesByName.put(activityName, step);
                workflowsByActivityName.put(activityName, workflow);

                if (step instanceof PartitionedWorkflowStep) {
                    String generatorActivityName = activityName + SfnArnFormatter.PARTITION_GENERATOR_ACTIVITY_SUFFIX;
                    activitiesByName.put(generatorActivityName, step);
                    workflowsByActivityName.put(generatorActivityName, workflow);
                }
            }
        }
    }

    // package-private for test access
    String buildSfnActivityName(Class<? extends Workflow> wfClass, Class<? extends WorkflowStep> stepClass) {
        return wfClass.getSimpleName() + "-" + stepClass.getSimpleName();
    }

    // package-private for testing
    void registerActivities() {
        try (MetricRecorder metrics = metricsFactory.newMetricRecorder("Flux.RegisterActivities")) {
            Set<String> registeredActivities
                    = AwsRetryUtils.executeWithInlineBackoff(this::describeRegisteredActivities, REGISTRATION_MAX_RETRY_ATTEMPTS,
                    REGISTRATION_MIN_RETRY_DELAY, REGISTRATION_MAX_RETRY_DELAY,
                    metrics, LIST_ACTIVITIES_METRIC_PREFIX);

            for (String activityName : activitiesByName.keySet()) {
                if (registeredActivities.contains(activityName)) {
                    log.info("Activity {} is already registered.", activityName);
                } else {
                    log.info("Registering activity {}", activityName);

                    CreateActivityRequest req = buildCreateActivityRequest(activityName);

                    AwsRetryUtils.executeWithInlineBackoff(() -> sfn.createActivity(req),
                            REGISTRATION_MAX_RETRY_ATTEMPTS,
                            REGISTRATION_MIN_RETRY_DELAY, REGISTRATION_MAX_RETRY_DELAY,
                            metrics, CREATE_ACTIVITY_METRIC_PREFIX);
                    log.info("Successfully registered activity {}", activityName);
                }
            }
        }
    }

    private Set<String> describeRegisteredActivities() {
        ListActivitiesRequest request = ListActivitiesRequest.builder().build();
        return sfn.listActivitiesPaginator(request).activities().stream()
                .map(ActivityListItem::name)
                .collect(Collectors.toSet());
    }

    static CreateActivityRequest buildCreateActivityRequest(String activityName) {
        return CreateActivityRequest.builder()
                .name(activityName)
                .build();
    }

    // package-private for testing
    void registerWorkflows() {
        if (workflowsByClass.isEmpty()) {
            return;
        }
        if (config.getStateMachineRoleArn() == null || config.getStateMachineRoleArn().isEmpty()) {
            throw new FluxException("stateMachineRoleArn must be set on FluxCapacitorConfig before workflows can be"
                                    + " registered.");
        }
        try (MetricRecorder metrics = metricsFactory.newMetricRecorder("Flux.RegisterWorkflows")) {
            for (Workflow workflow : workflowsByClass.values()) {
                registerOrUpdateWorkflow(workflow, metrics);
            }
        }
    }

    private void registerOrUpdateWorkflow(Workflow workflow, MetricRecorder metrics) {
        String workflowName = TaskNaming.workflowName(workflow);
        String stateMachineArn = SfnArnFormatter.workflowArn(config.getAwsRegion(),
                                                              config.getAwsAccountId(),
                                                              workflow.getClass());

        AslStateMachine asl = AslGenerator.generate(workflow, config.getAwsRegion(), config.getAwsAccountId());
        String desiredDefinition = asl.toJson();

        DescribeStateMachineResponse existing = describeStateMachine(stateMachineArn, metrics);

        if (existing == null) {
            log.info("Registering state machine for workflow {}", workflowName);
            CreateStateMachineRequest createReq = CreateStateMachineRequest.builder()
                    .name(workflowName)
                    .definition(desiredDefinition)
                    .roleArn(config.getStateMachineRoleArn())
                    .type(StateMachineType.STANDARD)
                    .publish(true)
                    .build();
            CreateStateMachineResponse response = AwsRetryUtils.executeWithInlineBackoff(
                    () -> sfn.createStateMachine(createReq),
                    REGISTRATION_MAX_RETRY_ATTEMPTS,
                    REGISTRATION_MIN_RETRY_DELAY, REGISTRATION_MAX_RETRY_DELAY,
                    metrics, CREATE_STATE_MACHINE_METRIC_PREFIX);
            log.info("Created state machine for workflow {}: arn={} version={}",
                     workflowName, response.stateMachineArn(), response.stateMachineVersionArn());
            return;
        }

        if (definitionMatches(existing.definition(), desiredDefinition)) {
            log.info("State machine for workflow {} is already registered with the desired definition.", workflowName);
            return;
        }

        if (!config.isUpdateExistingStateMachines()) {
            log.warn("State machine for workflow {} differs from Flux's generated definition, but "
                     + "updateExistingStateMachines is false; leaving the deployed definition as-is.",
                     workflowName);
            return;
        }

        log.info("Updating state machine for workflow {} (definition has changed).", workflowName);
        UpdateStateMachineRequest updateReq = UpdateStateMachineRequest.builder()
                .stateMachineArn(stateMachineArn)
                .definition(desiredDefinition)
                .roleArn(config.getStateMachineRoleArn())
                .publish(true)
                .build();
        AwsRetryUtils.executeWithInlineBackoff(
                () -> sfn.updateStateMachine(updateReq),
                REGISTRATION_MAX_RETRY_ATTEMPTS,
                REGISTRATION_MIN_RETRY_DELAY, REGISTRATION_MAX_RETRY_DELAY,
                metrics, UPDATE_STATE_MACHINE_METRIC_PREFIX);
        log.info("Updated state machine for workflow {}.", workflowName);
    }

    private DescribeStateMachineResponse describeStateMachine(String stateMachineArn, MetricRecorder metrics) {
        try {
            DescribeStateMachineRequest request = DescribeStateMachineRequest.builder()
                    .stateMachineArn(stateMachineArn).build();
            return AwsRetryUtils.executeWithInlineBackoff(
                    () -> sfn.describeStateMachine(request),
                    REGISTRATION_MAX_RETRY_ATTEMPTS,
                    REGISTRATION_MIN_RETRY_DELAY, REGISTRATION_MAX_RETRY_DELAY,
                    metrics, DESCRIBE_STATE_MACHINE_METRIC_PREFIX);
        } catch (StateMachineDoesNotExistException e) {
            return null;
        }
    }

    // package-private for testing
    static boolean definitionMatches(String existingJson, String desiredJson) {
        if (existingJson == null) {
            return false;
        }
        try {
            JsonNode existing = JSON_MAPPER.readTree(existingJson);
            JsonNode desired = JSON_MAPPER.readTree(desiredJson);
            return existing.equals(desired);
        } catch (JsonProcessingException e) {
            // If we can't parse the existing definition, treat it as a mismatch so we'll update it.
            log.warn("Could not parse existing state machine definition; will replace it.", e);
            return false;
        }
    }

    // useless to unit-test this method, all it does is start a bunch of threads that immediately try to poll for work
    private void initializePollers() {
        String hostname;
        try {
            hostname = shortenHostnameForIdentity(InetAddress.getLocalHost().getHostName());
        } catch (UnknownHostException e) {
            throw new RuntimeException("Unable to determine hostname", e);
        }

        hostname = config.getHostnameTransformerForPollerIdentity().apply(hostname);

        IdentifierValidation.validateHostname(hostname);

        workerThreadsPerTaskList = new HashMap<>();
        for (Workflow workflow : workflowsByClass.values()) {
            String taskList = workflow.taskList();
            int poolSize = config.getTaskListConfig(taskList).getActivityTaskThreadCount();
            workerThreadsPerTaskList.put(taskList, new BlockOnSubmissionThreadPoolExecutor(poolSize, "worker-" + taskList));
        }

        activityTaskPollerThreadsPerActivity = new HashMap<>();
        for (Workflow workflow : workflowsByClass.values()) {
            for (WorkflowGraphNode node : workflow.getGraph().getNodes().values()) {
                WorkflowStep step = node.getStep();
                createPollerForStep(workflow, step, hostname, false);
                if (step instanceof PartitionedWorkflowStep) {
                    createPollerForStep(workflow, step, hostname, true);
                }
            }
        }
    }

    private void createPollerForStep(Workflow workflow, WorkflowStep step, String hostname,
                                     boolean partitionGeneratorMode) {
        String activityName = buildSfnActivityName(workflow.getClass(), step.getClass())
                + (partitionGeneratorMode ? SfnArnFormatter.PARTITION_GENERATOR_ACTIVITY_SUFFIX : "");
        String activityArn = partitionGeneratorMode
                ? SfnArnFormatter.partitionGeneratorActivityArn(config.getAwsRegion(), config.getAwsAccountId(),
                                                                 workflow.getClass(), step.getClass())
                : SfnArnFormatter.activityArn(config.getAwsRegion(), config.getAwsAccountId(),
                                               workflow.getClass(), step.getClass());
        ScheduledExecutorService service = createActivityPollerPool(activityName, hostname,
                config.getTaskListConfig(workflow.taskList()).getActivityPollerThreadCount(),
                workerName -> new ActivityTaskPoller(metricsFactory, sfn, workerName, activityArn,
                        workflow, step, workerThreadsPerTaskList.get(workflow.taskList()),
                        partitionGeneratorMode));
        activityTaskPollerThreadsPerActivity.put(activityName, service);
    }

    /**
     * Creates a worker pool.
     *
     * @param activityName - The name of the activity this pool should poll for.
     * @param hostname     - The hostname of this worker.
     * @param poolSize     - The size of the pool
     * @param taskCreator  - A lambda that takes the full worker name as input and returns a new worker task to add to the pool.
     */
    private ScheduledExecutorService createActivityPollerPool(String activityName, String hostname, int poolSize,
                                                              Function<String, Runnable> taskCreator) {
        String poolName = String.format("poller-%s", activityName);
        ThreadFactory threadFactory = ThreadUtils.createStackTraceSuppressingThreadFactory(poolName);
        ScheduledExecutorService service = Executors.newScheduledThreadPool(poolSize, threadFactory);
        for (int i = 0; i < poolSize; i++) {
            // Since activity pollers are per-activity, the activity ARN is in the GetActivityTask request already.
            // So all we need to do is put the hostname and the thread number in the identity.
            String taskName = String.format("%s_%s", hostname, i);
            Runnable task = taskCreator.apply(taskName);
            // Scheduling the pollers so they will be restarted as soon as they end.
            service.scheduleWithFixedDelay(ThreadUtils.wrapInExceptionSwallower(task), 0, 10, TimeUnit.MILLISECONDS);
        }
        return service;
    }

    /**
     * Worker identity strings can't be too long so this method will trim out ".ec2.amazonaws.com" and similar.
     * Additional hostname transformations can be injected by setting
     * FluxCapacitorConfig's hostnameTransformerForPollerIdentity.
     *
     * Package-private for testing purposes.
     *
     * @param hostname The hostname to shorten
     * @return The shortened hostname
     */
    static String shortenHostnameForIdentity(String hostname) {
        String result = hostname.replace(".ec2.amazonaws.com.cn", "");
        result = result.replace(".ec2.amazonaws.com", "");
        result = result.replace(".compute.amazonaws.com.cn", "");
        result = result.replace(".compute.amazonaws.com", "");
        result = result.replace(".compute.internal", "");
        return result;
    }

    private void startPeriodicWorkflows() {
        int poolSize = Math.max(1, (int) workflowsByClass.values().stream()
                .filter(FluxCapacitorImpl::isPeriodicWorkflow).count());
        periodicWorkflowScheduler = Executors.newScheduledThreadPool(poolSize,
                ThreadUtils.createStackTraceSuppressingThreadFactory("periodicWorkflowSubmitter"));

        for (Workflow workflow : workflowsByClass.values()) {
            if (!isPeriodicWorkflow(workflow)) {
                continue;
            }

            Periodic periodic = workflow.getClass().getAnnotation(Periodic.class);
            long runIntervalSeconds = Math.max(1L,
                    TimeUnit.SECONDS.convert(periodic.runInterval(), periodic.intervalUnits()));
            long submitIntervalSeconds = computeSubmitIntervalSeconds(runIntervalSeconds);
            String workflowName = TaskNaming.workflowName(workflow);

            log.info("Scheduling periodic runs for workflow {} every {}s (run interval {}s).",
                     workflowName, submitIntervalSeconds, runIntervalSeconds);

            Runnable submitPeriodic = () -> submitPeriodicWorkflow(workflow, runIntervalSeconds);
            periodicWorkflowScheduler.scheduleAtFixedRate(ThreadUtils.wrapInExceptionSwallower(submitPeriodic),
                                                          0, submitIntervalSeconds, TimeUnit.SECONDS);
        }
    }

    /** Package-private for testing. */
    static boolean isPeriodicWorkflow(Workflow workflow) {
        return workflow.getClass().isAnnotationPresent(Periodic.class);
    }

    /**
     * Returns the cadence at which each worker should attempt a StartExecution for a periodic workflow.
     * Mirrors the SWF backend: half the run interval, clamped to [5s, 1h].
     */
    static long computeSubmitIntervalSeconds(long runIntervalSeconds) {
        return Math.min(Math.max(5L, runIntervalSeconds / 2L),
                        TimeUnit.SECONDS.convert(1, TimeUnit.HOURS));
    }

    /**
     * Constructs the deterministic SFN execution name used for a periodic-workflow submission landing in the
     * current run-interval window. Bucketing by run-interval gives natural fleet-wide deduplication: every worker
     * computing the name at roughly the same wall-clock moment lands on the same bucket index, and SFN's
     * uniqueness constraint on execution names lets exactly one of them win (the rest see
     * ExecutionAlreadyExistsException, which is treated as success).
     *
     * Bucket indices monotonically increase with wall-clock time, so they never collide with prior buckets
     * within the SFN 90-day reuse window.
     *
     * Package-private for testing.
     */
    static String periodicExecutionName(String workflowName, long bucketIndex) {
        return workflowName + "-" + bucketIndex;
    }

    private void submitPeriodicWorkflow(Workflow workflow, long runIntervalSeconds) {
        long bucketIndex = clock.instant().getEpochSecond() / runIntervalSeconds;
        String workflowName = TaskNaming.workflowName(workflow);
        String executionName = periodicExecutionName(workflowName, bucketIndex);
        try {
            executeWorkflow(workflow.getClass(), executionName, new HashMap<>());
        } catch (RuntimeException e) {
            // Periodic submissions must never let a transient failure kill the scheduler thread.
            log.warn("Failed to submit periodic workflow {} (bucket {}); will retry next interval.",
                     workflowName, bucketIndex, e);
        }
    }
}
