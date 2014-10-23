package com.dp.blackhole.rest;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mortbay.util.ajax.JSON;

@Path("/apps")
public class ApplicationResource extends BaseResource {
    private static final Log LOG = LogFactory.getLog(ApplicationResource.class);
    
    @GET
    @Path("/")
    @Produces({MediaType.APPLICATION_JSON})
    public Response getAllApps() {
        LOG.info("get: apps");
        String[] apps = configService.getAllCmdb().toArray(new String[configService.getAllCmdb().size()]);
        final String js = JSON.toString(apps);
        return Response.ok(js).build();
    }
    
    @GET
    @Path("/{app}/topics")
    @Produces({MediaType.APPLICATION_JSON})
    public Response getTopicsByApp(
            @PathParam("app") final String app) {
        LOG.info("get: app -> topics");
        Set<String> topicSet = configService.getTopicsByCmdb(app);
        String[] topics = topicSet.toArray(new String[topicSet.size()]);
        final String js = JSON.toString(topics);
        return Response.ok(js).build();
    }
    
    @GET
    @Path("/{app}/topics/{topic}/catalog")
    @Produces({MediaType.APPLICATION_JSON})
    public Response getCataLogByApp(
            @PathParam("app") final String app,
            @PathParam("topic") final String topic) {
        LOG.info("get: app -> catalog");
        List<String> apps = new ArrayList<String>();
        apps.add(app);
        String js = null;
        try {
            js = JSON.toString(configService.findInstancesByCmdbApps(topic, apps));
        } catch (Exception e) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity(e).build();
        }
        return Response.ok(js).build();
    }
}
