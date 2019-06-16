package org.apache.pulsar.functions.worker.rest.api;

import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.authentication.AuthenticationDataHttps;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.UpdateOptions;
import org.apache.pulsar.common.functions.Utils;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.common.io.SourceConfig;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.util.Codec;
import org.apache.pulsar.functions.auth.FunctionAuthData;
import org.apache.pulsar.functions.instance.InstanceUtils;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.utils.ComponentTypeUtils;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.apache.pulsar.functions.utils.FunctionConfigUtils;
import org.apache.pulsar.functions.utils.SinkConfigUtils;
import org.apache.pulsar.functions.utils.SourceConfigUtils;
import org.apache.pulsar.functions.worker.FunctionMetaDataManager;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.WorkerUtils;
import org.apache.pulsar.functions.worker.request.RequestResult;
import org.apache.pulsar.functions.worker.rest.RestException;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;

import javax.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.pulsar.functions.auth.FunctionAuthUtils.getFunctionAuthData;
import static org.apache.pulsar.functions.utils.FunctionCommon.getUniquePackageName;
import static org.apache.pulsar.functions.worker.WorkerUtils.isFunctionCodeBuiltin;
import static org.apache.pulsar.functions.worker.rest.RestUtils.throwUnavailableException;

@Slf4j
public abstract class RegisterComponent<C> extends ComponentAction {


    public RegisterComponent(WorkerService workerService, Function.FunctionDetails.ComponentType componentType) {
        super(workerService, componentType);
    }

    protected void register(final String tenant,
                            final String namespace,
                            final String componentName,
                            final InputStream uploadedInputStream,
                            final FormDataContentDisposition fileDetail,
                            final String functionPkgUrl,
                            final C componentConfig,
                            final String clientRole,
                            AuthenticationDataHttps clientAuthenticationDataHttps) {


        if (tenant == null) {
            throw new RestException(Response.Status.BAD_REQUEST, "Tenant is not provided");
        }
        if (namespace == null) {
            throw new RestException(Response.Status.BAD_REQUEST, "Namespace is not provided");
        }
        if (componentName == null) {
            throw new RestException(Response.Status.BAD_REQUEST, ComponentTypeUtils.toString(componentType) + " Name is not provided");
        }

        try {
            if (!isAuthorizedRole(tenant, namespace, clientRole, clientAuthenticationDataHttps)) {
                log.error("{}/{}/{} Client [{}] is not admin and authorized to register {}", tenant, namespace,
                        componentName, clientRole, ComponentTypeUtils.toString(componentType));
                throw new RestException(Response.Status.UNAUTHORIZED, "client is not authorize to perform operation");
            }
        } catch (PulsarAdminException e) {
            log.error("{}/{}/{} Failed to authorize [{}]", tenant, namespace, componentName, e);
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        try {
            // Check tenant exists
            final TenantInfo tenantInfo = worker().getBrokerAdmin().tenants().getTenantInfo(tenant);

            String qualifiedNamespace = tenant + "/" + namespace;
            List<String> namespaces = worker().getBrokerAdmin().namespaces().getNamespaces(tenant);
            if (namespaces != null && !namespaces.contains(qualifiedNamespace)) {
                String qualifiedNamespaceWithCluster = String.format("%s/%s/%s", tenant,
                        worker().getWorkerConfig().getPulsarFunctionsCluster(), namespace);
                if (namespaces != null && !namespaces.contains(qualifiedNamespaceWithCluster)) {
                    log.error("{}/{}/{} Namespace {} does not exist", tenant, namespace, componentName, namespace);
                    throw new RestException(Response.Status.BAD_REQUEST, "Namespace does not exist");
                }
            }
        } catch (PulsarAdminException.NotAuthorizedException e) {
            log.error("{}/{}/{} Client [{}] is not admin and authorized to operate {} on tenant", tenant, namespace,
                    componentName, clientRole, ComponentTypeUtils.toString(componentType));
            throw new RestException(Response.Status.UNAUTHORIZED, "client is not authorize to perform operation");
        } catch (PulsarAdminException.NotFoundException e) {
            log.error("{}/{}/{} Tenant {} does not exist", tenant, namespace, componentName, tenant);
            throw new RestException(Response.Status.BAD_REQUEST, "Tenant does not exist");
        } catch (PulsarAdminException e) {
            log.error("{}/{}/{} Issues getting tenant data", tenant, namespace, componentName, e);
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();

        if (functionMetaDataManager.containsFunction(tenant, namespace, componentName)) {
            log.error("{} {}/{}/{} already exists", ComponentTypeUtils.toString(componentType), tenant, namespace, componentName);
            throw new RestException(Response.Status.BAD_REQUEST, String.format("%s %s already exists", ComponentTypeUtils.toString(componentType), componentName));
        }

        Function.FunctionDetails functionDetails = null;
        boolean isPkgUrlProvided = isNotBlank(functionPkgUrl);
        File componentPackageFile = null;
        try {

            // validate parameters
            try {
                if (isPkgUrlProvided) {

                    if (!Utils.isFunctionPackageUrlSupported(functionPkgUrl)) {
                        throw new IllegalArgumentException("Function Package url is not valid. supported url (http/https/file)");
                    }
                    try {
                        componentPackageFile = FunctionCommon.extractFileFromPkgURL(functionPkgUrl);
                    } catch (Exception e) {
                        throw new IllegalArgumentException(String.format("Encountered error \"%s\" when getting %s package from %s", e.getMessage(), ComponentTypeUtils.toString(componentType), functionPkgUrl));
                    }
                    functionDetails = validateUpdateRequestParams(tenant, namespace, componentName,
                            componentConfig, componentPackageFile);
                } else {
                    if (uploadedInputStream != null) {
                        componentPackageFile = WorkerUtils.dumpToTmpFile(uploadedInputStream);
                    }
                    functionDetails = validateUpdateRequestParams(tenant, namespace, componentName,
                            componentConfig, componentPackageFile);
                    if (!isFunctionCodeBuiltin(functionDetails) && (componentPackageFile == null || fileDetail == null)) {
                        throw new IllegalArgumentException(ComponentTypeUtils.toString(componentType) + " Package is not provided");
                    }
                }
            } catch (Exception e) {
                log.error("Invalid register {} request @ /{}/{}/{}", ComponentTypeUtils.toString(componentType), tenant, namespace, componentName, e);
                throw new RestException(Response.Status.BAD_REQUEST, e.getMessage());
            }

            try {
                worker().getFunctionRuntimeManager().getRuntimeFactory().doAdmissionChecks(functionDetails);
            } catch (Exception e) {
                log.error("{} {}/{}/{} cannot be admitted by the runtime factory", ComponentTypeUtils.toString(componentType), tenant, namespace, componentName);
                throw new RestException(Response.Status.BAD_REQUEST, String.format("%s %s cannot be admitted:- %s", ComponentTypeUtils.toString(componentType), componentName, e.getMessage()));
            }

            // function state
            Function.FunctionMetaData.Builder functionMetaDataBuilder = Function.FunctionMetaData.newBuilder()
                    .setFunctionDetails(functionDetails)
                    .setCreateTime(System.currentTimeMillis())
                    .setVersion(0);

            // cache auth if need
            if (worker().getWorkerConfig().isAuthenticationEnabled()) {

                if (clientAuthenticationDataHttps != null) {
                    try {
                        Optional<FunctionAuthData> functionAuthData = worker().getFunctionRuntimeManager()
                                .getRuntimeFactory()
                                .getAuthProvider()
                                .cacheAuthData(tenant, namespace, componentName, clientAuthenticationDataHttps);

                        if (functionAuthData.isPresent()) {
                            functionMetaDataBuilder.setFunctionAuthSpec(
                                    Function.FunctionAuthenticationSpec.newBuilder()
                                            .setData(ByteString.copyFrom(functionAuthData.get().getData()))
                                            .build());
                        }
                    } catch (Exception e) {
                        log.error("Error caching authentication data for {} {}/{}/{}", ComponentTypeUtils.toString(componentType), tenant, namespace, componentName, e);


                        throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, String.format("Error caching authentication data for %s %s:- %s", ComponentTypeUtils.toString(componentType), componentName, e.getMessage()));
                    }
                }
            }

            Function.PackageLocationMetaData.Builder packageLocationMetaDataBuilder;
            try {
                packageLocationMetaDataBuilder = getFunctionPackageLocation(functionMetaDataBuilder.build(),
                        functionPkgUrl, fileDetail, componentPackageFile);
            } catch (Exception e) {
                log.error("Failed process {} {}/{}/{} package: ", ComponentTypeUtils.toString(componentType), tenant, namespace, componentName, e);
                throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
            }

            functionMetaDataBuilder.setPackageLocation(packageLocationMetaDataBuilder);
            updateRequest(functionMetaDataBuilder.build());
        } finally {

            if (!(functionPkgUrl != null && functionPkgUrl.startsWith(Utils.FILE))
                    && componentPackageFile != null && componentPackageFile.exists()) {
                componentPackageFile.delete();
            }
        }
    }

    protected abstract C getAndValidateMergeConfig(String tenant, String namespace, String componentName, C newComponentConfig);

    public void updateComponent(final String tenant,
                                final String namespace,
                                final String componentName,
                                final InputStream uploadedInputStream,
                                final FormDataContentDisposition fileDetail,
                                final String componentPkgUrl,
                                final C componentConfig,
                                final String clientRole,
                                final AuthenticationDataHttps clientAuthenticationDataHttps,
                                final UpdateOptions updateOptions) {

        if (tenant == null) {
            throw new RestException(Response.Status.BAD_REQUEST, "Tenant is not provided");
        }
        if (namespace == null) {
            throw new RestException(Response.Status.BAD_REQUEST, "Namespace is not provided");
        }
        if (componentName == null) {
            throw new RestException(Response.Status.BAD_REQUEST, ComponentTypeUtils.toString(componentType) + " Name is not provided");
        }

        try {
            if (!isAuthorizedRole(tenant, namespace, clientRole, clientAuthenticationDataHttps)) {
                log.error("{}/{}/{} Client [{}] is not admin and authorized to update {}", tenant, namespace,
                        componentName, clientRole, ComponentTypeUtils.toString(componentType));
                throw new RestException(Response.Status.UNAUTHORIZED, "client is not authorize to perform operation");

            }
        } catch (PulsarAdminException e) {
            log.error("{}/{}/{} Failed to authorize [{}]", tenant, namespace, componentName, e);
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();

        if (!functionMetaDataManager.containsFunction(tenant, namespace, componentName)) {
            throw new RestException(Response.Status.BAD_REQUEST, String.format("%s %s doesn't exist", ComponentTypeUtils.toString(componentType), componentName));
        }


        C mergedConfig = getAndValidateMergeConfig(tenant, namespace, componentName, componentConfig);

//        String mergedComponentConfigJson;
//        String existingComponentConfigJson;
//
//        Function.FunctionMetaData existingComponent = functionMetaDataManager.getFunctionMetaData(tenant, namespace, componentName);
//
//        if (!InstanceUtils.calculateSubjectType(existingComponent.getFunctionDetails()).equals(componentType)) {
//            log.error("{}/{}/{} is not a {}", tenant, namespace, componentName, ComponentTypeUtils.toString(componentType));
//            throw new RestException(Response.Status.NOT_FOUND, String.format("%s %s doesn't exist", ComponentTypeUtils.toString(componentType), componentName));
//        }
//
//        if (componentType.equals(Function.FunctionDetails.ComponentType.FUNCTION)) {
//            FunctionConfig existingFunctionConfig = FunctionConfigUtils.convertFromDetails(existingComponent.getFunctionDetails());
//            existingComponentConfigJson = new Gson().toJson(existingFunctionConfig);
//            FunctionConfig functionConfig = new Gson().fromJson(componentConfig, FunctionConfig.class);
//            // The rest end points take precedence over whatever is there in functionconfig
//            functionConfig.setTenant(tenant);
//            functionConfig.setNamespace(namespace);
//            functionConfig.setName(componentName);
//            try {
//                FunctionConfig mergedConfig = FunctionConfigUtils.validateUpdate(existingFunctionConfig,
//                        functionConfig);
//                mergedComponentConfigJson = new Gson().toJson(mergedConfig);
//            } catch (Exception e) {
//                throw new RestException(Response.Status.BAD_REQUEST, e.getMessage());
//            }
//        } else if (componentType.equals(Function.FunctionDetails.ComponentType.SOURCE)) {
//            SourceConfig existingSourceConfig = SourceConfigUtils.convertFromDetails(existingComponent.getFunctionDetails());
//            existingComponentConfigJson = new Gson().toJson(existingSourceConfig);
//            SourceConfig sourceConfig = new Gson().fromJson(componentConfig, SourceConfig.class);
//            // The rest end points take precedence over whatever is there in functionconfig
//            sourceConfig.setTenant(tenant);
//            sourceConfig.setNamespace(namespace);
//            sourceConfig.setName(componentName);
//            try {
//                SourceConfig mergedConfig = SourceConfigUtils.validateUpdate(existingSourceConfig, sourceConfig);
//                mergedComponentConfigJson = new Gson().toJson(mergedConfig);
//            } catch (Exception e) {
//                throw new RestException(Response.Status.BAD_REQUEST, e.getMessage());
//            }
//        } else {
//            SinkConfig existingSinkConfig = SinkConfigUtils.convertFromDetails(existingComponent.getFunctionDetails());
//            existingComponentConfigJson = new Gson().toJson(existingSinkConfig);
//            SinkConfig sinkConfig = new Gson().fromJson(componentConfig, SinkConfig.class);
//            // The rest end points take precedence over whatever is there in functionconfig
//            sinkConfig.setTenant(tenant);
//            sinkConfig.setNamespace(namespace);
//            sinkConfig.setName(componentName);
//            try {
//                SinkConfig mergedConfig = SinkConfigUtils.validateUpdate(existingSinkConfig, sinkConfig);
//                mergedComponentConfigJson = new Gson().toJson(mergedConfig);
//            } catch (Exception e) {
//                throw new RestException(Response.Status.BAD_REQUEST, e.getMessage());
//            }
//        }

        if (existingComponentConfigJson.equals(mergedComponentConfigJson) && isBlank(componentPkgUrl) && uploadedInputStream == null) {
            log.error("{}/{}/{} Update contains no changes", tenant, namespace, componentName);
            throw new RestException(Response.Status.BAD_REQUEST, "Update contains no change");
        }

        Function.FunctionDetails functionDetails = null;
        File componentPackageFile = null;
        try {

            // validate parameters
            try {
                if (isNotBlank(componentPkgUrl)) {
                    try {
                        componentPackageFile = FunctionCommon.extractFileFromPkgURL(componentPkgUrl);
                    } catch (Exception e) {
                        throw new IllegalArgumentException(String.format("Encountered error \"%s\" when getting %s package from %s", e.getMessage(), ComponentTypeUtils.toString(componentType), componentPkgUrl));
                    }
                    functionDetails = validateUpdateRequestParams(tenant, namespace, componentName,
                            componentConfig, componentPackageFile);

                } else if (existingComponent.getPackageLocation().getPackagePath().startsWith(Utils.FILE)
                        || existingComponent.getPackageLocation().getPackagePath().startsWith(Utils.HTTP)) {
                    try {
                        componentPackageFile = FunctionCommon.extractFileFromPkgURL(existingComponent.getPackageLocation().getPackagePath());
                    } catch (Exception e) {
                        throw new IllegalArgumentException(String.format("Encountered error \"%s\" when getting %s package from %s", e.getMessage(), ComponentTypeUtils.toString(componentType), componentPkgUrl));
                    }
                    functionDetails = validateUpdateRequestParams(tenant, namespace, componentName,
                            mergedComponentConfigJson, componentPackageFile);
                } else if (uploadedInputStream != null) {

                    componentPackageFile = WorkerUtils.dumpToTmpFile(uploadedInputStream);
                    functionDetails = validateUpdateRequestParams(tenant, namespace, componentName,
                            mergedComponentConfigJson, componentPackageFile);

                } else if (existingComponent.getPackageLocation().getPackagePath().startsWith(Utils.BUILTIN)) {
                    functionDetails = validateUpdateRequestParams(tenant, namespace, componentName,
                            mergedComponentConfigJson, componentPackageFile);
                    if (!isFunctionCodeBuiltin(functionDetails) && (componentPackageFile == null || fileDetail == null)) {
                        throw new IllegalArgumentException(ComponentTypeUtils.toString(componentType) + " Package is not provided");
                    }
                } else {

                    componentPackageFile = FunctionCommon.createPkgTempFile();
                    componentPackageFile.deleteOnExit();
                    log.info("componentPackageFile: {}", componentPackageFile);
                    WorkerUtils.downloadFromBookkeeper(worker().getDlogNamespace(), componentPackageFile, existingComponent.getPackageLocation().getPackagePath());

                    functionDetails = validateUpdateRequestParams(tenant, namespace, componentName,
                            mergedComponentConfigJson, componentPackageFile);
                }
            } catch (Exception e) {
                log.error("Invalid update {} request @ /{}/{}/{}", ComponentTypeUtils.toString(componentType), tenant, namespace, componentName, e);
                throw new RestException(Response.Status.BAD_REQUEST, e.getMessage());
            }

            try {
                worker().getFunctionRuntimeManager().getRuntimeFactory().doAdmissionChecks(functionDetails);
            } catch (Exception e) {
                log.error("Updated {} {}/{}/{} cannot be submitted to runtime factory", ComponentTypeUtils.toString(componentType), tenant, namespace, componentName);
                throw new RestException(Response.Status.BAD_REQUEST, String.format("%s %s cannot be admitted:- %s",
                        ComponentTypeUtils.toString(componentType), componentName, e.getMessage()));
            }

            // merge from existing metadata
            Function.FunctionMetaData.Builder functionMetaDataBuilder = Function.FunctionMetaData.newBuilder().mergeFrom(existingComponent)
                    .setFunctionDetails(functionDetails);

            // update auth data if need
            if (worker().getWorkerConfig().isAuthenticationEnabled()) {
                if (clientAuthenticationDataHttps != null && updateOptions != null && updateOptions.isUpdateAuthData()) {
                    // get existing auth data if it exists
                    Optional<FunctionAuthData> existingFunctionAuthData = Optional.empty();
                    if (functionMetaDataBuilder.hasFunctionAuthSpec()) {
                        existingFunctionAuthData = Optional.ofNullable(getFunctionAuthData(Optional.ofNullable(functionMetaDataBuilder.getFunctionAuthSpec())));
                    }

                    try {
                        Optional<FunctionAuthData> newFunctionAuthData = worker().getFunctionRuntimeManager()
                                .getRuntimeFactory()
                                .getAuthProvider()
                                .updateAuthData(
                                        tenant, namespace,
                                        componentName, existingFunctionAuthData,
                                        clientAuthenticationDataHttps);

                        if (newFunctionAuthData.isPresent()) {
                            functionMetaDataBuilder.setFunctionAuthSpec(
                                    Function.FunctionAuthenticationSpec.newBuilder()
                                            .setData(ByteString.copyFrom(newFunctionAuthData.get().getData()))
                                            .build());
                        } else {
                            functionMetaDataBuilder.clearFunctionAuthSpec();
                        }
                    } catch (Exception e) {
                        log.error("Error updating authentication data for {} {}/{}/{}", ComponentTypeUtils.toString(componentType), tenant, namespace, componentName, e);
                        throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, String.format("Error caching authentication data for %s %s:- %s", ComponentTypeUtils.toString(componentType), componentName, e.getMessage()));
                    }
                }
            }

            Function.PackageLocationMetaData.Builder packageLocationMetaDataBuilder;
            if (isNotBlank(componentPkgUrl) || uploadedInputStream != null) {
                try {
                    packageLocationMetaDataBuilder = getFunctionPackageLocation(functionMetaDataBuilder.build(),
                            componentPkgUrl, fileDetail, componentPackageFile);
                } catch (Exception e) {
                    log.error("Failed process {} {}/{}/{} package: ", ComponentTypeUtils.toString(componentType), tenant, namespace, componentName, e);
                    throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
                }
            } else {
                packageLocationMetaDataBuilder = Function.PackageLocationMetaData.newBuilder().mergeFrom(existingComponent.getPackageLocation());
            }

            functionMetaDataBuilder.setPackageLocation(packageLocationMetaDataBuilder);

            updateRequest(functionMetaDataBuilder.build());
        } finally {
            if (!(componentPkgUrl != null && componentPkgUrl.startsWith(Utils.FILE))
                    && componentPackageFile != null && componentPackageFile.exists()) {
                componentPackageFile.delete();
            }
        }
    }

    public abstract Function.FunctionDetails validateUpdateRequestParams(final String tenant,
                                                                         final String namespace,
                                                                         final String componentName,
                                                                         final C componentConfig,
                                                                         final File componentPackageFile) throws IOException;

    public Function.PackageLocationMetaData.Builder getFunctionPackageLocation(
            final Function.FunctionMetaData functionMetaData,
            final String functionPkgUrl,
            final FormDataContentDisposition fileDetail,
            final File uploadedInputStreamAsFile) throws Exception {
        Function.FunctionDetails functionDetails = functionMetaData.getFunctionDetails();
        String tenant = functionDetails.getTenant();
        String namespace = functionDetails.getNamespace();
        String componentName = functionDetails.getName();
        Function.PackageLocationMetaData.Builder packageLocationMetaDataBuilder = Function.PackageLocationMetaData.newBuilder();
        boolean isBuiltin = isFunctionCodeBuiltin(functionDetails);
        boolean isPkgUrlProvided = isNotBlank(functionPkgUrl);
        if (worker().getFunctionRuntimeManager().getRuntimeFactory().externallyManaged()) {
            // For externally managed schedulers, the pkgUrl/builtin stuff should be copied to bk
            if (isBuiltin) {
                File sinkOrSource;
                if (componentType == Function.FunctionDetails.ComponentType.SOURCE) {
                    String archiveName = functionDetails.getSource().getBuiltin();
                    sinkOrSource = worker().getConnectorsManager().getSourceArchive(archiveName).toFile();
                } else {
                    String archiveName = functionDetails.getSink().getBuiltin();
                    sinkOrSource = worker().getConnectorsManager().getSinkArchive(archiveName).toFile();
                }
                packageLocationMetaDataBuilder.setPackagePath(createPackagePath(tenant, namespace, componentName,
                        sinkOrSource.getName()));
                packageLocationMetaDataBuilder.setOriginalFileName(sinkOrSource.getName());
                log.info("Uploading {} package to {}", ComponentTypeUtils.toString(componentType), packageLocationMetaDataBuilder.getPackagePath());
                WorkerUtils.uploadFileToBookkeeper(packageLocationMetaDataBuilder.getPackagePath(), sinkOrSource, worker().getDlogNamespace());
            } else if (isPkgUrlProvided) {
                packageLocationMetaDataBuilder.setPackagePath(createPackagePath(tenant, namespace, componentName,
                        uploadedInputStreamAsFile.getName()));
                packageLocationMetaDataBuilder.setOriginalFileName(uploadedInputStreamAsFile.getName());
                log.info("Uploading {} package to {}", ComponentTypeUtils.toString(componentType), packageLocationMetaDataBuilder.getPackagePath());
                WorkerUtils.uploadFileToBookkeeper(packageLocationMetaDataBuilder.getPackagePath(), uploadedInputStreamAsFile, worker().getDlogNamespace());
            } else if (functionMetaData.getPackageLocation().getPackagePath().startsWith(Utils.HTTP)
                    || functionMetaData.getPackageLocation().getPackagePath().startsWith(Utils.FILE)) {
                String fileName = new File(new URL(functionMetaData.getPackageLocation().getPackagePath()).toURI()).getName();
                packageLocationMetaDataBuilder.setPackagePath(createPackagePath(tenant, namespace, componentName,
                        fileName));
                packageLocationMetaDataBuilder.setOriginalFileName(fileName);
                log.info("Uploading {} package to {}", ComponentTypeUtils.toString(componentType), packageLocationMetaDataBuilder.getPackagePath());
                WorkerUtils.uploadFileToBookkeeper(packageLocationMetaDataBuilder.getPackagePath(), uploadedInputStreamAsFile, worker().getDlogNamespace());
            } else {
                packageLocationMetaDataBuilder.setPackagePath(createPackagePath(tenant, namespace, componentName,
                        fileDetail.getFileName()));
                packageLocationMetaDataBuilder.setOriginalFileName(fileDetail.getFileName());
                log.info("Uploading {} package to {}", ComponentTypeUtils.toString(componentType), packageLocationMetaDataBuilder.getPackagePath());
                WorkerUtils.uploadFileToBookkeeper(packageLocationMetaDataBuilder.getPackagePath(), uploadedInputStreamAsFile, worker().getDlogNamespace());
            }
        } else {
            // For pulsar managed schedulers, the pkgUrl/builtin stuff should be copied to bk
            if (isBuiltin) {
                packageLocationMetaDataBuilder.setPackagePath("builtin://" + getFunctionCodeBuiltin(functionDetails));
            } else if (isPkgUrlProvided) {
                packageLocationMetaDataBuilder.setPackagePath(functionPkgUrl);
            } else if (functionMetaData.getPackageLocation().getPackagePath().startsWith(Utils.HTTP)
                    || functionMetaData.getPackageLocation().getPackagePath().startsWith(Utils.FILE)) {
                packageLocationMetaDataBuilder.setPackagePath(functionMetaData.getPackageLocation().getPackagePath());
            } else {
                packageLocationMetaDataBuilder.setPackagePath(createPackagePath(tenant, namespace, componentName, fileDetail.getFileName()));
                packageLocationMetaDataBuilder.setOriginalFileName(fileDetail.getFileName());
                log.info("Uploading {} package to {}", ComponentTypeUtils.toString(componentType), packageLocationMetaDataBuilder.getPackagePath());
                WorkerUtils.uploadFileToBookkeeper(packageLocationMetaDataBuilder.getPackagePath(), uploadedInputStreamAsFile, worker().getDlogNamespace());
            }
        }
        return packageLocationMetaDataBuilder;
    }

    private String getFunctionCodeBuiltin(Function.FunctionDetails functionDetails) {
        if (functionDetails.hasSource()) {
            Function.SourceSpec sourceSpec = functionDetails.getSource();
            if (!isEmpty(sourceSpec.getBuiltin())) {
                return sourceSpec.getBuiltin();
            }
        }

        if (functionDetails.hasSink()) {
            Function.SinkSpec sinkSpec = functionDetails.getSink();
            if (!isEmpty(sinkSpec.getBuiltin())) {
                return sinkSpec.getBuiltin();
            }
        }

        return null;
    }

    public static String createPackagePath(String tenant, String namespace, String functionName, String fileName) {
        return String.format("%s/%s/%s/%s", tenant, namespace, Codec.encode(functionName),
                getUniquePackageName(Codec.encode(fileName)));
    }

    private void updateRequest(final Function.FunctionMetaData functionMetaData) {

        // Submit to FMT
        CompletableFuture<RequestResult> completableFuture = worker().getFunctionMetaDataManager().updateFunction(functionMetaData);

        RequestResult requestResult = null;
        try {
            requestResult = completableFuture.get();
            if (!requestResult.isSuccess()) {
                throw new RestException(Response.Status.BAD_REQUEST, requestResult.getMessage());
            }
        } catch (ExecutionException e) {
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        } catch (InterruptedException e) {
            throw new RestException(Response.Status.REQUEST_TIMEOUT, e.getMessage());
        }
    }
}
