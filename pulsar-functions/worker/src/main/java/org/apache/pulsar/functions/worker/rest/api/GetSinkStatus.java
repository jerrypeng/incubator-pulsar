package org.apache.pulsar.functions.worker.rest.api;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.ExceptionInformation;
import org.apache.pulsar.common.policies.data.SinkStatus;
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
 * Class for retrieving the status of Pulsar Sinks
 */
@Slf4j
class GetSinkStatus extends GetComponentStatus<SinkStatus, SinkStatus.SinkInstanceStatus.SinkInstanceStatusData> {


    public GetSinkStatus(WorkerService workerService) {
        super(workerService, Function.FunctionDetails.ComponentType.SINK);
    }

    @Override
    protected SinkStatus.SinkInstanceStatus.SinkInstanceStatusData notScheduledInstance() {
        SinkStatus.SinkInstanceStatus.SinkInstanceStatusData sinkInstanceStatusData
                = new SinkStatus.SinkInstanceStatus.SinkInstanceStatusData();
        sinkInstanceStatusData.setRunning(false);
        sinkInstanceStatusData.setError("Sink has not been scheduled");
        return sinkInstanceStatusData;
    }

    @Override
    protected SinkStatus.SinkInstanceStatus.SinkInstanceStatusData fromFunctionStatusProto(
            InstanceCommunication.FunctionStatus status,
            String assignedWorkerId) {
        SinkStatus.SinkInstanceStatus.SinkInstanceStatusData sinkInstanceStatusData
                = new SinkStatus.SinkInstanceStatus.SinkInstanceStatusData();
        sinkInstanceStatusData.setRunning(status.getRunning());
        sinkInstanceStatusData.setError(status.getFailureException());
        sinkInstanceStatusData.setNumRestarts(status.getNumRestarts());
        sinkInstanceStatusData.setNumReadFromPulsar(status.getNumReceived());

        // We treat source/user/system exceptions returned from function as system exceptions
        sinkInstanceStatusData.setNumSystemExceptions(status.getNumSystemExceptions()
                + status.getNumUserExceptions() + status.getNumSourceExceptions());
        List<ExceptionInformation> systemExceptionInformationList = new LinkedList<>();
        for (InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry : status.getLatestUserExceptionsList()) {
            ExceptionInformation exceptionInformation = getExceptionInformation(exceptionEntry);
            systemExceptionInformationList.add(exceptionInformation);
        }

        for (InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry : status.getLatestSystemExceptionsList()) {
            ExceptionInformation exceptionInformation = getExceptionInformation(exceptionEntry);
            systemExceptionInformationList.add(exceptionInformation);
        }

        for (InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry : status.getLatestSourceExceptionsList()) {
            ExceptionInformation exceptionInformation = getExceptionInformation(exceptionEntry);
            systemExceptionInformationList.add(exceptionInformation);
        }
        sinkInstanceStatusData.setLatestSystemExceptions(systemExceptionInformationList);

        sinkInstanceStatusData.setNumSinkExceptions(status.getNumSinkExceptions());
        List<ExceptionInformation> sinkExceptionInformationList = new LinkedList<>();
        for (InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry : status.getLatestSinkExceptionsList()) {
            ExceptionInformation exceptionInformation = getExceptionInformation(exceptionEntry);
            sinkExceptionInformationList.add(exceptionInformation);
        }
        sinkInstanceStatusData.setLatestSinkExceptions(sinkExceptionInformationList);

        sinkInstanceStatusData.setNumWrittenToSink(status.getNumSuccessfullyProcessed());
        sinkInstanceStatusData.setLastReceivedTime(status.getLastInvocationTime());
        sinkInstanceStatusData.setWorkerId(assignedWorkerId);

        return sinkInstanceStatusData;
    }

    @Override
    protected SinkStatus.SinkInstanceStatus.SinkInstanceStatusData notRunning(String assignedWorkerId, String error) {
        SinkStatus.SinkInstanceStatus.SinkInstanceStatusData sinkInstanceStatusData
                = new SinkStatus.SinkInstanceStatus.SinkInstanceStatusData();
        sinkInstanceStatusData.setRunning(false);
        if (error != null) {
            sinkInstanceStatusData.setError(error);
        }
        sinkInstanceStatusData.setWorkerId(assignedWorkerId);

        return sinkInstanceStatusData;
    }

    @Override
    protected SinkStatus getStatus(final String tenant,
                                final String namespace,
                                final String name,
                                final Collection<Function.Assignment> assignments,
                                final URI uri) throws PulsarAdminException {
        SinkStatus sinkStatus = new SinkStatus();
        for (Function.Assignment assignment : assignments) {
            boolean isOwner = worker().getWorkerConfig().getWorkerId().equals(assignment.getWorkerId());
            SinkStatus.SinkInstanceStatus.SinkInstanceStatusData sinkInstanceStatusData;
            if (isOwner) {
                sinkInstanceStatusData = getComponentInstanceStatus(tenant,
                        namespace, name, assignment.getInstance().getInstanceId(), null);
            } else {
                sinkInstanceStatusData = worker().getFunctionAdmin().sinks().getSinkStatus(
                        assignment.getInstance().getFunctionMetaData().getFunctionDetails().getTenant(),
                        assignment.getInstance().getFunctionMetaData().getFunctionDetails().getNamespace(),
                        assignment.getInstance().getFunctionMetaData().getFunctionDetails().getName(),
                        assignment.getInstance().getInstanceId());
            }

            SinkStatus.SinkInstanceStatus instanceStatus = new SinkStatus.SinkInstanceStatus();
            instanceStatus.setInstanceId(assignment.getInstance().getInstanceId());
            instanceStatus.setStatus(sinkInstanceStatusData);
            sinkStatus.addInstance(instanceStatus);
        }

        sinkStatus.setNumInstances(sinkStatus.instances.size());
        sinkStatus.getInstances().forEach(sinkInstanceStatus -> {
            if (sinkInstanceStatus.getStatus().isRunning()) {
                sinkStatus.numRunning++;
            }
        });
        return sinkStatus;
    }

    @Override
    protected SinkStatus getStatusExternal(final String tenant,
                                        final String namespace,
                                        final String name,
                                        final int parallelism) {
        SinkStatus sinkStatus = new SinkStatus();
        for (int i = 0; i < parallelism; ++i) {
            SinkStatus.SinkInstanceStatus.SinkInstanceStatusData sinkInstanceStatusData
                    = getComponentInstanceStatus(tenant, namespace, name, i, null);
            SinkStatus.SinkInstanceStatus sinkInstanceStatus
                    = new SinkStatus.SinkInstanceStatus();
            sinkInstanceStatus.setInstanceId(i);
            sinkInstanceStatus.setStatus(sinkInstanceStatusData);
            sinkStatus.addInstance(sinkInstanceStatus);
        }

        sinkStatus.setNumInstances(sinkStatus.instances.size());
        sinkStatus.getInstances().forEach(sinkInstanceStatus -> {
            if (sinkInstanceStatus.getStatus().isRunning()) {
                sinkStatus.numRunning++;
            }
        });
        return sinkStatus;
    }

    @Override
    protected SinkStatus emptyStatus(final int parallelism) {
        SinkStatus sinkStatus = new SinkStatus();
        sinkStatus.setNumInstances(parallelism);
        sinkStatus.setNumRunning(0);
        for (int i = 0; i < parallelism; i++) {
            SinkStatus.SinkInstanceStatus sinkInstanceStatus = new SinkStatus.SinkInstanceStatus();
            sinkInstanceStatus.setInstanceId(i);
            SinkStatus.SinkInstanceStatus.SinkInstanceStatusData sinkInstanceStatusData
                    = new SinkStatus.SinkInstanceStatus.SinkInstanceStatusData();
            sinkInstanceStatusData.setRunning(false);
            sinkInstanceStatusData.setError("Sink has not been scheduled");
            sinkInstanceStatus.setStatus(sinkInstanceStatusData);

            sinkStatus.addInstance(sinkInstanceStatus);
        }

        return sinkStatus;
    }

    public SinkStatus.SinkInstanceStatus.SinkInstanceStatusData getSinkInstanceStatus(final String tenant,
                                                                                      final String namespace,
                                                                                      final String sinkName,
                                                                                      final String instanceId,
                                                                                      final URI uri,
                                                                                      final String clientRole,
                                                                                      final AuthenticationDataSource clientAuthenticationDataHttps) {

        // validate parameters
        componentInstanceStatusRequestValidate(tenant, namespace, sinkName, Integer.parseInt(instanceId), clientRole, clientAuthenticationDataHttps);


        SinkStatus.SinkInstanceStatus.SinkInstanceStatusData sinkInstanceStatusData;
        try {
            sinkInstanceStatusData = getComponentInstanceStatus(tenant, namespace, sinkName,
                    Integer.parseInt(instanceId), uri);
        } catch (WebApplicationException we) {
            throw we;
        } catch (Exception e) {
            log.error("{}/{}/{} Got Exception Getting Status", tenant, namespace, sinkName, e);
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }
        return sinkInstanceStatusData;
    }

    public SinkStatus getSinkStatus(final String tenant,
                                    final String namespace,
                                    final String componentName,
                                    final URI uri,
                                    final String clientRole,
                                    final AuthenticationDataSource clientAuthenticationDataHttps) {

        // validate parameters
        componentStatusRequestValidate(tenant, namespace, componentName, clientRole, clientAuthenticationDataHttps);

        SinkStatus sinkStatus;
        try {
            sinkStatus = getComponentStatus(tenant, namespace, componentName, uri);
        } catch (WebApplicationException we) {
            throw we;
        } catch (Exception e) {
            log.error("{}/{}/{} Got Exception Getting Status", tenant, namespace, componentName, e);
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        return sinkStatus;
    }
}
