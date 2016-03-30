package com.dp.blackhole.rest;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.Encoded;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mortbay.util.ajax.JSON;

import com.dp.blackhole.common.Util;
import com.dp.blackhole.supervisor.model.Stream;

@Path("/ops")
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
            @Encoded @FormParam("source") final String source,
            @Encoded @FormParam("force") final String force) {
        LOG.debug("POST: retire " + topic + " " + source);
        boolean forceBoolean = false;
        if (force != null && !force.isEmpty()) {
            forceBoolean = Util.parseBoolean(force, false);
        }
        return supervisorService.retireStream(topic, source, forceBoolean)
                ? Response.ok().build() : Response.status(Response.Status.NOT_ACCEPTABLE).build();
    }
    
    @POST
    @Path("/restart")
    @Produces({MediaType.APPLICATION_JSON})
    public Response restart(
            @Encoded @FormParam("agents") final String agents) {
        LOG.debug("POST: restart agents " + agents);
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
    
    @GET
    @Path("/snapshot/{opname}/{topic}/{source}")
    @Produces({MediaType.APPLICATION_JSON})
    public Response logSnapshot(
            @PathParam("opname") final String opname,
            @PathParam("topic") final String topic,
            @PathParam("source") final String source) {
        LOG.debug("GET: snapshot op " + opname + " topic " + topic + " source " + source);
        return supervisorService.oprateSnapshot(topic, source, opname)
            ? Response.ok().build() : Response.status(Response.Status.BAD_REQUEST).build();
    }
    
    @POST
    @Path("/pause")
    @Produces({MediaType.APPLICATION_JSON})
    public Response pauseStream(
            @Encoded @FormParam("topic") final String topic,
            @Encoded @FormParam("source") final String source,
            @Encoded @FormParam("delay") final String delay) {
        LOG.debug("POST: pause " + topic + " " + source + " in " + delay + " seconds");
        return supervisorService.pauseStream(topic, source, Util.parseInt(delay, 30))
                ? Response.ok().build() : Response.status(Response.Status.BAD_REQUEST).build();
    }
    
    @GET
    @Path("/checkpoint/view")
    @Produces({MediaType.APPLICATION_JSON})
    public Response checkpiontView() {
        LOG.debug("GET: checkpoint view asynchronously");
        File checkpointViewFile = new File("/tmp/checkpoint_view." + Util.getTS());
        byte[] checkpiontBytes = supervisorService.checkpoint.getStatus().toString().getBytes();
        FileOutputStream oStream = null;
        try {
            oStream = new FileOutputStream(checkpointViewFile);
            oStream.write(checkpiontBytes);
            return Response.ok().build();
        } catch (IOException e) {
            return Response.status(Response.Status.NOT_FOUND).build();
        } finally {
            if (oStream != null) {
                try {
                    oStream.close();
                } catch (IOException e1) {
                }
            }
        }
    }
}
