package org.disertatie.dbsync;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Response;

@Path("/db_example")
public class Endpoints {

    @GET
    @Path("/")
    public Response aas() {
        return Response.ok().build();

    }

    @GET
    @Path("/aaa")
    public Response aaa() {
        return Response.ok().build();

    }
}
