package com.dp.blackhole.rest;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.Encoded;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpException;

import com.dp.blackhole.common.ParamsKey;
import com.dp.blackhole.supervisor.model.Blacklist;
import com.dp.blackhole.supervisor.model.TopicConfig;

@Path("/confs")
public class TopicConfsResource extends BaseResource {
    private static final Log LOG = LogFactory.getLog(TopicConfsResource.class);
    
    @GET
    @Path("/")
    @Produces({MediaType.APPLICATION_JSON})
    public List<TopicConfig> getAllConfInfos() {
        LOG.info("GET: all confs");
        List<TopicConfig> allConfs = new ArrayList<TopicConfig>();
        for (String topic : configService.getAllTopicConfNames()) {
            allConfs.add(configService.getConfByTopic(topic));
        }
        return allConfs;
    }
    
    @GET
    @Path("/{topic}")
    @Produces({MediaType.APPLICATION_JSON})
    public TopicConfig getConfInfoByTopic(
            @PathParam("topic") final String topic) {
        LOG.info("GET: topic -> conf");
        return configService.getConfByTopic(topic);
    }
    
    @DELETE
    @Path("/{topic}")
    @Produces({MediaType.APPLICATION_JSON})
    public Response deleteConfByTopic(
            @PathParam("topic") final String topic) {
        LOG.info("DEL: topic -> conf");
        configService.removeConf(topic);
        return Response.ok().build();
    }
    
    @GET
    @Path("/blacklist")
    @Produces({MediaType.APPLICATION_JSON})
    public Blacklist listBlacklist() {
        LOG.info("GET: blacklist");
        return configService.getBlacklist();
    }
    
    @PUT
    @Path("/blacklist/{topic}")
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces({MediaType.APPLICATION_JSON})
    public Response updateBlacklist(
            @PathParam("topic") final String topic, 
            @Encoded @HeaderParam("op") final String op) {
        LOG.info("PUT: topic " + op + " blacklist");
        String[] updates = {topic};
        try {
            configService.updateLionList(ParamsKey.LionNode.BLACKLIST, op, updates);
        } catch (HttpException e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(e).build();
        }
        return Response.ok().build();
    }
}
