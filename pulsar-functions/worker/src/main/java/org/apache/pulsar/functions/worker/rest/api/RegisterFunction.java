package org.apache.pulsar.functions.worker.rest.api;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.common.io.SourceConfig;
import org.apache.pulsar.functions.instance.InstanceUtils;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.utils.ComponentTypeUtils;
import org.apache.pulsar.functions.utils.FunctionConfigUtils;
import org.apache.pulsar.functions.utils.SinkConfigUtils;
import org.apache.pulsar.functions.utils.SourceConfigUtils;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.rest.RestException;

import javax.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;

@Slf4j
public class RegisterFunction extends RegisterComponent<FunctionConfig> {

    public RegisterFunction(WorkerService workerService) {
        super(workerService, Function.FunctionDetails.ComponentType.FUNCTION);
    }

    @Override
    protected FunctionConfig getAndValidateMergeConfig(String tenant, String namespace, String componentName,
                                                       FunctionConfig newFunctionConfig) {
        Function.FunctionMetaData existingComponent = worker().getFunctionMetaDataManager().getFunctionMetaData(tenant, namespace, componentName);

        FunctionConfig existingFunctionConfig = FunctionConfigUtils.convertFromDetails(existingComponent.getFunctionDetails());
        // The rest end points take precedence over whatever is there in function config
        newFunctionConfig.setTenant(tenant);
        newFunctionConfig.setNamespace(namespace);
        newFunctionConfig.setName(componentName);
        try {
            return FunctionConfigUtils.validateUpdate(existingFunctionConfig, newFunctionConfig);
        } catch (Exception e) {
            throw new RestException(Response.Status.BAD_REQUEST, e.getMessage());
        }
    }

    @Override
    public Function.FunctionDetails validateUpdateRequestParams(
            String tenant, String namespace, String functionName,
            FunctionConfig functionConfig, File functionPackageFile) throws IOException {
        // The rest end points take precedence over whatever is there in functionconfig
        functionConfig.setTenant(tenant);
        functionConfig.setNamespace(namespace);
        functionConfig.setName(functionName);
        FunctionConfigUtils.inferMissingArguments(functionConfig);
        ClassLoader clsLoader = FunctionConfigUtils.validate(functionConfig, functionPackageFile);
        return FunctionConfigUtils.convert(functionConfig, clsLoader);
    }
}
