package org.apache.pulsar.functions.worker.rest.api;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.authentication.AuthenticationDataHttps;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.functions.UpdateOptions;
import org.apache.pulsar.common.io.SourceConfig;
import org.apache.pulsar.common.policies.data.ExceptionInformation;
import org.apache.pulsar.common.policies.data.SinkStatus;
import org.apache.pulsar.common.policies.data.SourceStatus;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.worker.FunctionMetaDataManager;
import org.apache.pulsar.functions.worker.FunctionRuntimeManager;
import org.apache.pulsar.functions.worker.MembershipManager;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.rest.RestException;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.net.URI;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Class for retrieving the status of Pulsar Sinks
 */
@Slf4j
class GetSourceStatus extends GetComponentStatus<SourceStatus, SourceStatus.SourceInstanceStatus.SourceInstanceStatusData> {


    public GetSourceStatus(WorkerService workerService) {
        super(workerService, Function.FunctionDetails.ComponentType.SOURCE);
    }

    @Override
    protected SourceStatus.SourceInstanceStatus.SourceInstanceStatusData notScheduledInstance() {
        SourceStatus.SourceInstanceStatus.SourceInstanceStatusData sourceInstanceStatusData
                = new SourceStatus.SourceInstanceStatus.SourceInstanceStatusData();
        sourceInstanceStatusData.setRunning(false);
        sourceInstanceStatusData.setError("Source has not been scheduled");
        return sourceInstanceStatusData;
    }

    @Override
    protected SourceStatus.SourceInstanceStatus.SourceInstanceStatusData fromFunctionStatusProto(
            InstanceCommunication.FunctionStatus status,
            String assignedWorkerId) {
        SourceStatus.SourceInstanceStatus.SourceInstanceStatusData sourceInstanceStatusData
                = new SourceStatus.SourceInstanceStatus.SourceInstanceStatusData();
        sourceInstanceStatusData.setRunning(status.getRunning());
        sourceInstanceStatusData.setError(status.getFailureException());
        sourceInstanceStatusData.setNumRestarts(status.getNumRestarts());
        sourceInstanceStatusData.setNumReceivedFromSource(status.getNumReceived());

        sourceInstanceStatusData.setNumSourceExceptions(status.getNumSourceExceptions());
        List<ExceptionInformation> sourceExceptionInformationList = new LinkedList<>();
        for (InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry : status.getLatestSourceExceptionsList()) {
            ExceptionInformation exceptionInformation
                    = new ExceptionInformation();
            exceptionInformation.setTimestampMs(exceptionEntry.getMsSinceEpoch());
            exceptionInformation.setExceptionString(exceptionEntry.getExceptionString());
            sourceExceptionInformationList.add(exceptionInformation);
        }
        sourceInstanceStatusData.setLatestSourceExceptions(sourceExceptionInformationList);

        // Source treats all system and sink exceptions as system exceptions
        sourceInstanceStatusData.setNumSystemExceptions(status.getNumSystemExceptions()
                + status.getNumUserExceptions() + status.getNumSinkExceptions());
        List<ExceptionInformation> systemExceptionInformationList = new LinkedList<>();
        for (InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry : status.getLatestUserExceptionsList()) {
            ExceptionInformation exceptionInformation
                    = new ExceptionInformation();
            exceptionInformation.setTimestampMs(exceptionEntry.getMsSinceEpoch());
            exceptionInformation.setExceptionString(exceptionEntry.getExceptionString());
            systemExceptionInformationList.add(exceptionInformation);
        }

        for (InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry : status.getLatestSystemExceptionsList()) {
            ExceptionInformation exceptionInformation
                    = new ExceptionInformation();
            exceptionInformation.setTimestampMs(exceptionEntry.getMsSinceEpoch());
            exceptionInformation.setExceptionString(exceptionEntry.getExceptionString());
            systemExceptionInformationList.add(exceptionInformation);
        }

        for (InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry : status.getLatestSinkExceptionsList()) {
            ExceptionInformation exceptionInformation
                    = new ExceptionInformation();
            exceptionInformation.setTimestampMs(exceptionEntry.getMsSinceEpoch());
            exceptionInformation.setExceptionString(exceptionEntry.getExceptionString());
            systemExceptionInformationList.add(exceptionInformation);
        }
        sourceInstanceStatusData.setLatestSystemExceptions(systemExceptionInformationList);

        sourceInstanceStatusData.setNumWritten(status.getNumSuccessfullyProcessed());
        sourceInstanceStatusData.setLastReceivedTime(status.getLastInvocationTime());
        sourceInstanceStatusData.setWorkerId(assignedWorkerId);

        return sourceInstanceStatusData;
    }

    @Override
    protected SourceStatus.SourceInstanceStatus.SourceInstanceStatusData notRunning(String assignedWorkerId, String error) {
        SourceStatus.SourceInstanceStatus.SourceInstanceStatusData sourceInstanceStatusData
                = new SourceStatus.SourceInstanceStatus.SourceInstanceStatusData();
        sourceInstanceStatusData.setRunning(false);
        if (error != null) {
            sourceInstanceStatusData.setError(error);
        }
        sourceInstanceStatusData.setWorkerId(assignedWorkerId);

        return sourceInstanceStatusData;
    }

    @Override
    protected SourceStatus getStatus(final String tenant,
                                  final String namespace,
                                  final String name,
                                  final Collection<Function.Assignment> assignments,
                                  final URI uri) throws PulsarAdminException {
        SourceStatus sourceStatus = new SourceStatus();
        for (Function.Assignment assignment : assignments) {
            boolean isOwner = worker().getWorkerConfig().getWorkerId().equals(assignment.getWorkerId());
            SourceStatus.SourceInstanceStatus.SourceInstanceStatusData sourceInstanceStatusData;
            if (isOwner) {
                sourceInstanceStatusData = getComponentInstanceStatus(tenant, namespace, name, assignment.getInstance().getInstanceId(), null);
            } else {
                sourceInstanceStatusData = worker().getFunctionAdmin().sources().getSourceStatus(
                        assignment.getInstance().getFunctionMetaData().getFunctionDetails().getTenant(),
                        assignment.getInstance().getFunctionMetaData().getFunctionDetails().getNamespace(),
                        assignment.getInstance().getFunctionMetaData().getFunctionDetails().getName(),
                        assignment.getInstance().getInstanceId());
            }

            SourceStatus.SourceInstanceStatus instanceStatus = new SourceStatus.SourceInstanceStatus();
            instanceStatus.setInstanceId(assignment.getInstance().getInstanceId());
            instanceStatus.setStatus(sourceInstanceStatusData);
            sourceStatus.addInstance(instanceStatus);
        }

        sourceStatus.setNumInstances(sourceStatus.instances.size());
        sourceStatus.getInstances().forEach(sourceInstanceStatus -> {
            if (sourceInstanceStatus.getStatus().isRunning()) {
                sourceStatus.numRunning++;
            }
        });
        return sourceStatus;
    }

    @Override
    protected SourceStatus getStatusExternal(final String tenant,
                                          final String namespace,
                                          final String name,
                                          final int parallelism) {
        SourceStatus sinkStatus = new SourceStatus();
        for (int i = 0; i < parallelism; ++i) {
            SourceStatus.SourceInstanceStatus.SourceInstanceStatusData sourceInstanceStatusData
                    = getComponentInstanceStatus(tenant, namespace, name, i, null);
            SourceStatus.SourceInstanceStatus sourceInstanceStatus
                    = new SourceStatus.SourceInstanceStatus();
            sourceInstanceStatus.setInstanceId(i);
            sourceInstanceStatus.setStatus(sourceInstanceStatusData);
            sinkStatus.addInstance(sourceInstanceStatus);
        }

        sinkStatus.setNumInstances(sinkStatus.instances.size());
        sinkStatus.getInstances().forEach(sourceInstanceStatus -> {
            if (sourceInstanceStatus.getStatus().isRunning()) {
                sinkStatus.numRunning++;
            }
        });
        return sinkStatus;
    }

    @Override
    protected SourceStatus emptyStatus(final int parallelism) {
        SourceStatus sourceStatus = new SourceStatus();
        sourceStatus.setNumInstances(parallelism);
        sourceStatus.setNumRunning(0);
        for (int i = 0; i < parallelism; i++) {
            SourceStatus.SourceInstanceStatus sourceInstanceStatus = new SourceStatus.SourceInstanceStatus();
            sourceInstanceStatus.setInstanceId(i);
            SourceStatus.SourceInstanceStatus.SourceInstanceStatusData sourceInstanceStatusData
                    = new SourceStatus.SourceInstanceStatus.SourceInstanceStatusData();
            sourceInstanceStatusData.setRunning(false);
            sourceInstanceStatusData.setError("Source has not been scheduled");
            sourceInstanceStatus.setStatus(sourceInstanceStatusData);

            sourceStatus.addInstance(sourceInstanceStatus);
        }

        return sourceStatus;
    }

    public SourceStatus getSourceStatus(final String tenant,
                                        final String namespace,
                                        final String componentName,
                                        final URI uri, final String clientRole,
                                        final AuthenticationDataSource clientAuthenticationDataHttps) {
        // validate parameters
        componentStatusRequestValidate(tenant, namespace, componentName, clientRole, clientAuthenticationDataHttps);

        SourceStatus sourceStatus;
        try {
            sourceStatus = getComponentStatus(tenant, namespace, componentName, uri);
        } catch (WebApplicationException we) {
            throw we;
        } catch (Exception e) {
            log.error("{}/{}/{} Got Exception Getting Status", tenant, namespace, componentName, e);
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        return sourceStatus;
    }

    public SourceStatus.SourceInstanceStatus.SourceInstanceStatusData getSourceInstanceStatus(final String tenant,
                                                                                              final String namespace,
                                                                                              final String sourceName,
                                                                                              final String instanceId,
                                                                                              final URI uri,
                                                                                              final String clientRole,
                                                                                              final AuthenticationDataSource clientAuthenticationDataHttps) {
        // validate parameters
        componentInstanceStatusRequestValidate(tenant, namespace, sourceName, Integer.parseInt(instanceId), clientRole, clientAuthenticationDataHttps);

        SourceStatus.SourceInstanceStatus.SourceInstanceStatusData sourceInstanceStatusData;
        try {
            sourceInstanceStatusData = getComponentInstanceStatus(tenant, namespace, sourceName,
                    Integer.parseInt(instanceId), uri);
        } catch (WebApplicationException we) {
            throw we;
        } catch (Exception e) {
            log.error("{}/{}/{} Got Exception Getting Status", tenant, namespace, sourceName, e);
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }
        return sourceInstanceStatusData;
    }
}
