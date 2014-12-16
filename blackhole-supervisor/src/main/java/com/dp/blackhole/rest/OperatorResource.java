package com.dp.blackhole.rest;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.Encoded;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mortbay.util.ajax.JSON;

import com.dp.blackhole.supervisor.model.Stream;

@Path("ops")
public class OperatorResource extends BaseResource {
    private static final Log LOG = LogFactory.getLog(OperatorResource.class);
    
    @POST
    @Path("/recovery")
    @Produces({MediaType.APPLICATION_JSON})
    public Response recovery(
            @Encoded @FormParam("topic") final String topic,
            @Encoded @FormParam("source") final String source,
            @FormParam("rollTs") final long rollTs) {
        LOG.debug("POST: recovery " + topic + " " + source + " " + Long.toString(rollTs));
        return supervisorService.manualRecoveryRoll(topic, source, rollTs)
                ? Response.ok().build() : Response.status(Response.Status.NOT_ACCEPTABLE).build();
    }
    
    @POST
    @Path("/range")
    @Produces({MediaType.APPLICATION_JSON})
    public Response range(
            @Encoded @FormParam("topic") final String topic,
            @Encoded @FormParam("source") final String source,
            @FormParam("start") final long startRollTs,
            @FormParam("end") final long endRollTs) {
        LOG.debug("POST: range " + topic + " " + source + " from " + Long.toString(startRollTs) + " to " + Long.toString(endRollTs));
        Stream stream = supervisorService.getStream(topic, source);
        if (stream != null) {
            long period = stream.getPeriod();
            long recoveryStageCount = (endRollTs - startRollTs) / period / 1000;
            for (int i = 0; i<= recoveryStageCount; i++) {
                long rollTs = startRollTs + period * 1000 * (i);
                if (!supervisorService.manualRecoveryRoll(topic, source, rollTs)) {
                    LOG.warn(topic + " " + source + " " + rollTs + " can not recovery.");
                }
            }
            return Response.ok().build();
        } else {
            return Response.status(Response.Status.NOT_ACCEPTABLE).build();
        }
        
    }
    
    @POST
    @Path("/retire")
    @Produces({MediaType.APPLICATION_JSON})
    public Response retire(
            @Encoded @FormParam("topic") final String topic,
            @Encoded @FormParam("source") final String source) {
        LOG.debug("POST: retire " + topic + " " + source);
        return supervisorService.retireStream(topic, source)
                ? Response.ok().build() : Response.status(Response.Status.NOT_ACCEPTABLE).build();
    }
    
    @POST
    @Path("/restart")
    @Produces({MediaType.APPLICATION_JSON})
    @Consumes({MediaType.APPLICATION_JSON})
    public Response restart(
            @Encoded @FormParam("agents") final String agents) {
        try {
            Object[] raw = (Object[])JSON.parse(agents);
            List<String> agentServers = new ArrayList<String>();
            for (int i = 0; i < raw.length; i++) {
                agentServers.add((String)raw[i]);
            }
            supervisorService.sendRestart(agentServers);
            return Response.ok().build();
        } catch (Exception e) {
            LOG.error(e.getMessage());
            return Response.status(Response.Status.BAD_REQUEST).entity(e.getMessage()).build();
        }
        
    }
    
    @GET
    @Path("/download/{source}")
    @Produces({MediaType.APPLICATION_JSON})
    public Response download(
            @Encoded @FormParam("source") final String source) {
        return Response.ok().build();
    }
}
