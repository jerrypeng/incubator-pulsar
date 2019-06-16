package org.apache.pulsar.functions.worker.rest.api;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.ExceptionInformation;
import org.apache.pulsar.common.policies.data.FunctionStatus;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.rest.RestException;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * Class for retrieving status of Pulsar Functions
 */
@Slf4j
class GetFunctionStatus extends GetComponentStatus<FunctionStatus, FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData> {

    public GetFunctionStatus(WorkerService workerService) {
        super(workerService, Function.FunctionDetails.ComponentType.FUNCTION);
    }

    @Override
    protected FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData notScheduledInstance() {
        FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData functionInstanceStatusData
                = new FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData();
        functionInstanceStatusData.setRunning(false);
        functionInstanceStatusData.setError("Function has not been scheduled");
        return functionInstanceStatusData;
    }

    @Override
    protected FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData fromFunctionStatusProto(
            InstanceCommunication.FunctionStatus status,
            String assignedWorkerId) {
        FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData functionInstanceStatusData
                = new FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData();
        functionInstanceStatusData.setRunning(status.getRunning());
        functionInstanceStatusData.setError(status.getFailureException());
        functionInstanceStatusData.setNumRestarts(status.getNumRestarts());
        functionInstanceStatusData.setNumReceived(status.getNumReceived());
        functionInstanceStatusData.setNumSuccessfullyProcessed(status.getNumSuccessfullyProcessed());
        functionInstanceStatusData.setNumUserExceptions(status.getNumUserExceptions());

        List<ExceptionInformation> userExceptionInformationList = new LinkedList<>();
        for (InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry : status.getLatestUserExceptionsList()) {
            ExceptionInformation exceptionInformation = getExceptionInformation(exceptionEntry);
            userExceptionInformationList.add(exceptionInformation);
        }
        functionInstanceStatusData.setLatestUserExceptions(userExceptionInformationList);

        // For regular functions source/sink errors are system exceptions
        functionInstanceStatusData.setNumSystemExceptions(status.getNumSystemExceptions()
                + status.getNumSourceExceptions() + status.getNumSinkExceptions());
        List<ExceptionInformation> systemExceptionInformationList = new LinkedList<>();
        for (InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry : status.getLatestSystemExceptionsList()) {
            ExceptionInformation exceptionInformation = getExceptionInformation(exceptionEntry);
            systemExceptionInformationList.add(exceptionInformation);
        }
        for (InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry : status.getLatestSourceExceptionsList()) {
            ExceptionInformation exceptionInformation = getExceptionInformation(exceptionEntry);
            systemExceptionInformationList.add(exceptionInformation);
        }
        for (InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry : status.getLatestSinkExceptionsList()) {
            ExceptionInformation exceptionInformation = getExceptionInformation(exceptionEntry);
            systemExceptionInformationList.add(exceptionInformation);
        }
        functionInstanceStatusData.setLatestSystemExceptions(systemExceptionInformationList);

        functionInstanceStatusData.setAverageLatency(status.getAverageLatency());
        functionInstanceStatusData.setLastInvocationTime(status.getLastInvocationTime());
        functionInstanceStatusData.setWorkerId(assignedWorkerId);

        return functionInstanceStatusData;
    }

    @Override
    protected FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData notRunning(String assignedWorkerId, String error) {
        FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData functionInstanceStatusData
                = new FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData();
        functionInstanceStatusData.setRunning(false);
        if (error != null) {
            functionInstanceStatusData.setError(error);
        }
        functionInstanceStatusData.setWorkerId(assignedWorkerId);

        return functionInstanceStatusData;
    }

    @Override
    protected FunctionStatus getStatus(String tenant, String namespace, String name, Collection<Function.Assignment>
            assignments, URI uri) throws PulsarAdminException {
        FunctionStatus functionStatus = new FunctionStatus();
        for (Function.Assignment assignment : assignments) {
            boolean isOwner = worker().getWorkerConfig().getWorkerId().equals(assignment.getWorkerId());
            FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData functionInstanceStatusData;
            if (isOwner) {
                functionInstanceStatusData = getComponentInstanceStatus(tenant, namespace, name, assignment
                        .getInstance().getInstanceId(), null);
            } else {
                functionInstanceStatusData = worker().getFunctionAdmin().functions().getFunctionStatus(
                        assignment.getInstance().getFunctionMetaData().getFunctionDetails().getTenant(),
                        assignment.getInstance().getFunctionMetaData().getFunctionDetails().getNamespace(),
                        assignment.getInstance().getFunctionMetaData().getFunctionDetails().getName(),
                        assignment.getInstance().getInstanceId());
            }

            FunctionStatus.FunctionInstanceStatus instanceStatus = new FunctionStatus.FunctionInstanceStatus();
            instanceStatus.setInstanceId(assignment.getInstance().getInstanceId());
            instanceStatus.setStatus(functionInstanceStatusData);
            functionStatus.addInstance(instanceStatus);
        }

        functionStatus.setNumInstances(functionStatus.instances.size());
        functionStatus.getInstances().forEach(functionInstanceStatus -> {
            if (functionInstanceStatus.getStatus().isRunning()) {
                functionStatus.numRunning++;
            }
        });
        return functionStatus;
    }

    @Override
    protected FunctionStatus getStatusExternal(final String tenant,
                                            final String namespace,
                                            final String name,
                                            final int parallelism) {
        FunctionStatus functionStatus = new FunctionStatus();
        for (int i = 0; i < parallelism; ++i) {
            FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData functionInstanceStatusData
                    = getComponentInstanceStatus(tenant, namespace, name, i, null);
            FunctionStatus.FunctionInstanceStatus functionInstanceStatus
                    = new FunctionStatus.FunctionInstanceStatus();
            functionInstanceStatus.setInstanceId(i);
            functionInstanceStatus.setStatus(functionInstanceStatusData);
            functionStatus.addInstance(functionInstanceStatus);
        }

        functionStatus.setNumInstances(functionStatus.instances.size());
        functionStatus.getInstances().forEach(functionInstanceStatus -> {
            if (functionInstanceStatus.getStatus().isRunning()) {
                functionStatus.numRunning++;
            }
        });
        return functionStatus;
    }

    @Override
    protected FunctionStatus emptyStatus(final int parallelism) {
        FunctionStatus functionStatus = new FunctionStatus();
        functionStatus.setNumInstances(parallelism);
        functionStatus.setNumRunning(0);
        for (int i = 0; i < parallelism; i++) {
            FunctionStatus.FunctionInstanceStatus functionInstanceStatus = new FunctionStatus.FunctionInstanceStatus();
            functionInstanceStatus.setInstanceId(i);
            FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData functionInstanceStatusData
                    = new FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData();
            functionInstanceStatusData.setRunning(false);
            functionInstanceStatusData.setError("Function has not been scheduled");
            functionInstanceStatus.setStatus(functionInstanceStatusData);

            functionStatus.addInstance(functionInstanceStatus);
        }

        return functionStatus;
    }

    public FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData getFunctionInstanceStatus(
            final String tenant, final String namespace, final String componentName, final String instanceId,
            final URI uri, final String clientRole, final AuthenticationDataSource clientAuthenticationDataHttps) {

        // validate parameters
        componentInstanceStatusRequestValidate(tenant, namespace, componentName, Integer.parseInt(instanceId), clientRole, clientAuthenticationDataHttps);

        FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData functionInstanceStatusData;
        try {

            functionInstanceStatusData = getComponentInstanceStatus(tenant, namespace, componentName,
                    Integer.parseInt(instanceId), uri);
        } catch (WebApplicationException we) {
            throw we;
        } catch (Exception e) {
            log.error("{}/{}/{} Got Exception Getting Status", tenant, namespace, componentName, e);
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        return functionInstanceStatusData;
    }

    public FunctionStatus getFunctionStatus(final String tenant,
                                            final String namespace,
                                            final String functionName,
                                            final URI uri,
                                            final String clientRole,
                                            final AuthenticationDataSource clientAuthenticationDataHttps) {
        componentStatusRequestValidate(tenant, namespace, functionName, clientRole, clientAuthenticationDataHttps);

        FunctionStatus functionStatus;
        try {
            functionStatus = getComponentStatus(tenant, namespace, functionName, uri);
        } catch (WebApplicationException we) {
            throw we;
        } catch (Exception e) {
            log.error("{}/{}/{} Got Exception Getting Status", tenant, namespace, functionName, e);
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        return functionStatus;
    }
}
