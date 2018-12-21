package org.apache.pulsar.broker.admin.v3;

import io.swagger.annotations.Api;
import javax.ws.rs.Consumes;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.pulsar.broker.admin.impl.FunctionsBase;

@Path("/functions")
@Api(value = "/functions", description = "Functions admin apis", tags = "functions", hidden = true)
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class Functions extends FunctionsBase {

}