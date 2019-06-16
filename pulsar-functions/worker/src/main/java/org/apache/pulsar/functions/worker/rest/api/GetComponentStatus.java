package org.apache.pulsar.functions.worker.rest.api;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.functions.WorkerInfo;
import org.apache.pulsar.common.policies.data.ExceptionInformation;
import org.apache.pulsar.functions.instance.InstanceUtils;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.runtime.RuntimeSpawner;
import org.apache.pulsar.functions.utils.ComponentTypeUtils;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.apache.pulsar.functions.worker.FunctionMetaDataManager;
import org.apache.pulsar.functions.worker.FunctionRuntimeInfo;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.rest.RestException;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Class for retrieving the status of components i.e. Pulsar Functions, Sources, and Sinks
 */
@Slf4j
abstract class GetComponentStatus<S, T> extends ComponentAction {

    public GetComponentStatus(WorkerService workerService, Function.FunctionDetails.ComponentType componentType) {
        super(workerService, componentType);
    }

    protected abstract T notScheduledInstance();

    protected abstract T fromFunctionStatusProto(final InstanceCommunication.FunctionStatus status,
                                              final String assignedWorkerId);

    protected abstract T notRunning(final String assignedWorkerId, final String error);

    protected T getComponentInstanceStatus(final String tenant,
                                        final String namespace,
                                        final String name,
                                        final int instanceId,
                                        final URI uri) {

        Function.Assignment assignment;
        if (worker().getFunctionRuntimeManager().getRuntimeFactory().externallyManaged()) {
            assignment = worker().getFunctionRuntimeManager().findFunctionAssignment(tenant, namespace, name, -1);
        } else {
            assignment = worker().getFunctionRuntimeManager().findFunctionAssignment(tenant, namespace, name, instanceId);
        }

        if (assignment == null) {
            return notScheduledInstance();
        }

        final String assignedWorkerId = assignment.getWorkerId();
        final String workerId = worker().getWorkerConfig().getWorkerId();

        // If I am running worker
        if (assignedWorkerId.equals(workerId)) {
            FunctionRuntimeInfo functionRuntimeInfo = worker().getFunctionRuntimeManager().getFunctionRuntimeInfo(
                    FunctionCommon.getFullyQualifiedInstanceId(assignment.getInstance()));
            if (functionRuntimeInfo == null) {
                return notRunning(assignedWorkerId, "");
            }
            RuntimeSpawner runtimeSpawner = functionRuntimeInfo.getRuntimeSpawner();

            if (runtimeSpawner != null) {
                try {
                    return fromFunctionStatusProto(
                            functionRuntimeInfo.getRuntimeSpawner().getFunctionStatus(instanceId).get(),
                            assignedWorkerId);
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            } else {
                String message = functionRuntimeInfo.getStartupException() != null ? functionRuntimeInfo.getStartupException().getMessage() : "";
                return notRunning(assignedWorkerId, message);
            }
        } else {
            // query other worker

            List<WorkerInfo> workerInfoList = worker().getMembershipManager().getCurrentMembership();
            WorkerInfo workerInfo = null;
            for (WorkerInfo entry : workerInfoList) {
                if (assignment.getWorkerId().equals(entry.getWorkerId())) {
                    workerInfo = entry;
                }
            }
            if (workerInfo == null) {
                return notScheduledInstance();
            }

            if (uri == null) {
                throw new WebApplicationException(Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build());
            } else {
                URI redirect = UriBuilder.fromUri(uri).host(workerInfo.getWorkerHostname()).port(workerInfo.getPort()).build();
                throw new WebApplicationException(Response.temporaryRedirect(redirect).build());
            }
        }
    }

    protected abstract S getStatus(final String tenant,
                                final String namespace,
                                final String name,
                                final Collection<Function.Assignment> assignments,
                                final URI uri) throws PulsarAdminException;

    protected abstract S getStatusExternal(final String tenant,
                                        final String namespace,
                                        final String name,
                                        final int parallelism);

    protected abstract S emptyStatus(final int parallelism);

    protected S getComponentStatus(final String tenant,
                                final String namespace,
                                final String name,
                                final URI uri) {

        Function.FunctionMetaData functionMetaData = worker().getFunctionMetaDataManager().getFunctionMetaData(tenant, namespace, name);

        Collection<Function.Assignment> assignments = worker().getFunctionRuntimeManager().findFunctionAssignments(tenant, namespace, name);

        // TODO refactor the code for externally managed.
        if (worker().getFunctionRuntimeManager().getRuntimeFactory().externallyManaged()) {
            Function.Assignment assignment = assignments.iterator().next();
            boolean isOwner = worker().getWorkerConfig().getWorkerId().equals(assignment.getWorkerId());
            if (isOwner) {
                return getStatusExternal(tenant, namespace, name, functionMetaData.getFunctionDetails().getParallelism());
            } else {

                // find the hostname/port of the worker who is the owner

                List<WorkerInfo> workerInfoList = worker().getMembershipManager().getCurrentMembership();
                WorkerInfo workerInfo = null;
                for (WorkerInfo entry: workerInfoList) {
                    if (assignment.getWorkerId().equals(entry.getWorkerId())) {
                        workerInfo = entry;
                    }
                }
                if (workerInfo == null) {
                    return emptyStatus(functionMetaData.getFunctionDetails().getParallelism());
                }

                if (uri == null) {
                    throw new WebApplicationException(Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build());
                } else {
                    URI redirect = UriBuilder.fromUri(uri).host(workerInfo.getWorkerHostname()).port(workerInfo.getPort()).build();
                    throw new WebApplicationException(Response.temporaryRedirect(redirect).build());
                }
            }
        } else {
            try {
                return getStatus(tenant, namespace, name, assignments, uri);
            } catch (PulsarAdminException e) {
                throw new RuntimeException(e);
            }
        }
    }

    protected ExceptionInformation getExceptionInformation(InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry) {
        ExceptionInformation exceptionInformation
                = new ExceptionInformation();
        exceptionInformation.setTimestampMs(exceptionEntry.getMsSinceEpoch());
        exceptionInformation.setExceptionString(exceptionEntry.getExceptionString());
        return exceptionInformation;
    }

    protected void componentInstanceStatusRequestValidate (final String tenant,
                                                           final String namespace,
                                                           final String componentName,
                                                           final int instanceId,
                                                           final String clientRole,
                                                           final AuthenticationDataSource clientAuthenticationDataHttps) {
        componentStatusRequestValidate(tenant, namespace, componentName, clientRole, clientAuthenticationDataHttps);

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();
        Function.FunctionMetaData functionMetaData = functionMetaDataManager.getFunctionMetaData(tenant, namespace, componentName);
        int parallelism = functionMetaData.getFunctionDetails().getParallelism();
        if (instanceId < 0 || instanceId >= parallelism) {
            log.error("instanceId in get {} Status out of bounds @ /{}/{}/{}", ComponentTypeUtils.toString(componentType), tenant, namespace, componentName);
            throw new RestException(Response.Status.BAD_REQUEST,
                    String.format("%s %s doesn't have instance with id %s", ComponentTypeUtils.toString(componentType), componentName, instanceId));
        }
    }

    protected void componentStatusRequestValidate (final String tenant, final String namespace, final String componentName,
                                                   final String clientRole,
                                                   final AuthenticationDataSource clientAuthenticationDataHttps) {

        try {
            if (!isAuthorizedRole(tenant, namespace, clientRole, clientAuthenticationDataHttps)) {
                log.error("{}/{}/{} Client [{}] is not admin and authorized get status for {}", tenant, namespace,
                        componentName, clientRole, ComponentTypeUtils.toString(componentType));
                throw new RestException(Response.Status.UNAUTHORIZED, "client is not authorize to perform operation");
            }
        } catch (PulsarAdminException e) {
            log.error("{}/{}/{} Failed to authorize [{}]", tenant, namespace, componentName, e);
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        // validate parameters
        try {
            validateGetFunctionRequestParams(tenant, namespace, componentName, componentType);
        } catch (IllegalArgumentException e) {
            log.error("Invalid get {} Status request @ /{}/{}/{}", ComponentTypeUtils.toString(componentType), tenant, namespace, componentName, e);
            throw new RestException(Response.Status.BAD_REQUEST, e.getMessage());
        }

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();
        if (!functionMetaDataManager.containsFunction(tenant, namespace, componentName)) {
            log.error("{} in get {} Status does not exist @ /{}/{}/{}", ComponentTypeUtils.toString(componentType), componentType, tenant, namespace, componentName);
            throw new RestException(Response.Status.NOT_FOUND, String.format("%s %s doesn't exist", ComponentTypeUtils.toString(componentType), componentName));
        }

        Function.FunctionMetaData functionMetaData = functionMetaDataManager.getFunctionMetaData(tenant, namespace, componentName);
        if (!InstanceUtils.calculateSubjectType(functionMetaData.getFunctionDetails()).equals(componentType)) {
            log.error("{}/{}/{} is not a {}", tenant, namespace, componentName, ComponentTypeUtils.toString(componentType));
            throw new RestException(Response.Status.NOT_FOUND, String.format("%s %s doesn't exist", ComponentTypeUtils.toString(componentType), componentName));
        }
    }
}
