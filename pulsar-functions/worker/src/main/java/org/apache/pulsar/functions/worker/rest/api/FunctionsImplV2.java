package org.apache.pulsar.functions.worker.rest.api;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.policies.data.ErrorData;
import org.apache.pulsar.common.policies.data.FunctionStatus;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.worker.FunctionMetaDataManager;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.rest.RestException;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Slf4j
public class FunctionsImplV2 extends FunctionsImpl{
    public FunctionsImplV2(Supplier<WorkerService> workerServiceSupplier) {
        super(workerServiceSupplier);
    }

    public Response getFunctionInfoV2(final String tenant, final String namespace, final String functionName)
            throws IOException {

        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        // validate parameters
        try {
            validateGetFunctionRequestParams(tenant, namespace, functionName, componentType);
        } catch (IllegalArgumentException e) {
            log.error("Invalid get {} request @ /{}/{}/{}", componentType, tenant, namespace, functionName, e);
            throw new RestException(Response.Status.BAD_REQUEST, e.getMessage());
        }

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();
        if (!functionMetaDataManager.containsFunction(tenant, namespace, functionName)) {
            log.error("{} does not exist @ /{}/{}/{}", componentType, tenant, namespace, functionName);
            throw new RestException(Response.Status.NOT_FOUND, String.format(componentType + " %s doesn't exist", functionName));
        }

        Function.FunctionMetaData functionMetaData = functionMetaDataManager.getFunctionMetaData(tenant, namespace,
                functionName);
        String functionDetailsJson = org.apache.pulsar.functions.utils.Utils
                .printJson(functionMetaData.getFunctionDetails());
        return Response.status(Response.Status.OK).entity(functionDetailsJson).build();
    }

    public Response getFunctionInstanceStatusV2(final String tenant, final String namespace, final String functionName,
                                              final String instanceId, URI uri) throws IOException {

        org.apache.pulsar.common.policies.data.FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData
                functionInstanceStatus = this.getFunctionInstanceStatus(tenant, namespace, functionName, instanceId, uri);

        String jsonResponse = org.apache.pulsar.functions.utils.Utils.printJson(toProto(functionInstanceStatus, instanceId));
        return Response.status(Response.Status.OK).entity(jsonResponse).build();
    }

    public Response getFunctionStatusV2(String tenant, String namespace, String functionName, URI requestUri) throws
            IOException {
        FunctionStatus functionStatus = this.getFunctionStatus(tenant, namespace, functionName, requestUri);
        InstanceCommunication.FunctionStatusList.Builder functionStatusList = InstanceCommunication.FunctionStatusList.newBuilder();
        functionStatus.instances.forEach(functionInstanceStatus -> functionStatusList.addFunctionStatusList(
                toProto(functionInstanceStatus.getStatus(),
                String.valueOf(functionInstanceStatus.getInstanceId()))));
        String jsonResponse = org.apache.pulsar.functions.utils.Utils.printJson(functionStatusList);
        return Response.status(Response.Status.OK).entity(jsonResponse).build();
    }

    private Response getUnavailableResponse() {
        return Response.status(Response.Status.SERVICE_UNAVAILABLE).type(MediaType.APPLICATION_JSON)
                .entity(new ErrorData(
                        "Function worker service is not done initializing. " + "Please try again in a little while."))
                .build();
    }

    private InstanceCommunication.FunctionStatus toProto(
            org.apache.pulsar.common.policies.data.FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData
                    functionInstanceStatus, String instanceId) {
        List<InstanceCommunication.FunctionStatus.ExceptionInformation> latestSysExceptions
                = functionInstanceStatus.getLatestSystemExceptions()
                .stream()
                .map(exceptionInformation -> InstanceCommunication.FunctionStatus.ExceptionInformation.newBuilder()
                        .setExceptionString(exceptionInformation.getExceptionString())
                        .setMsSinceEpoch(exceptionInformation.getTimestampMs())
                        .build())
                .collect(Collectors.toList());

        List<InstanceCommunication.FunctionStatus.ExceptionInformation> latestUserExceptions
                = functionInstanceStatus.getLatestUserExceptions()
                .stream()
                .map(exceptionInformation -> InstanceCommunication.FunctionStatus.ExceptionInformation.newBuilder()
                        .setExceptionString(exceptionInformation.getExceptionString())
                        .setMsSinceEpoch(exceptionInformation.getTimestampMs())
                        .build())
                .collect(Collectors.toList());


        InstanceCommunication.FunctionStatus functionStatus = InstanceCommunication.FunctionStatus.newBuilder()
                .setRunning(functionInstanceStatus.isRunning())
                .setFailureException(functionInstanceStatus.getError())
                .setNumRestarts(functionInstanceStatus.getNumRestarts())
                .setNumSuccessfullyProcessed(functionInstanceStatus.getNumSuccessfullyProcessed())
                .setNumUserExceptions(functionInstanceStatus.getNumUserExceptions())
                .addAllLatestUserExceptions(latestUserExceptions)
                .setNumSystemExceptions(functionInstanceStatus.getNumSystemExceptions())
                .addAllLatestSystemExceptions(latestSysExceptions)
                .setAverageLatency(functionInstanceStatus.getAverageLatency())
                .setLastInvocationTime(functionInstanceStatus.getLastInvocationTime())
                .setInstanceId(instanceId)
                .setWorkerId(worker().getWorkerConfig().getWorkerId())
                .build();

        return functionStatus;
    }
}
