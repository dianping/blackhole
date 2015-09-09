package com.dp.blackhole.rest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.Encoded;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
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
import com.dp.blackhole.supervisor.model.Topic;
import com.dp.blackhole.supervisor.model.TopicConfig;
import com.dp.blackhole.supervisor.model.Stream.StreamId;

@Path("/confs")
public class TopicConfsResource extends BaseResource {
    private static final Log LOG = LogFactory.getLog(TopicConfsResource.class);
    
    @GET
    @Path("/")
    @Produces({MediaType.APPLICATION_JSON})
    public List<TopicConfig> getAllConfInfos() {
        LOG.debug("GET: all confs");
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
        LOG.debug("GET: topic[" + topic + "] -> conf");
        return configService.getConfByTopic(topic);
    }
    
    @DELETE
    @Path("/{topic}")
    @Produces({MediaType.APPLICATION_JSON})
    public Response deleteConfByTopic(
            @PathParam("topic") final String topic) {
        LOG.debug("DEL: topic[" + topic + "] -> conf");
        configService.removeConf(topic);
        return Response.ok().build();
    }
    
    @GET
    @Path("/blacklist")
    @Produces({MediaType.APPLICATION_JSON})
    public Blacklist listBlacklist() {
        LOG.debug("GET: blacklist");
        return configService.getBlacklist();
    }
    
    @PUT
    @Path("/blacklist/{topic}")
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces({MediaType.APPLICATION_JSON})
    public Response updateBlacklist(
            @PathParam("topic") final String topic, 
            @Encoded @FormParam("op") final String op) {
        LOG.debug("PUT: topic[" + topic + "] " + op + " blacklist");
        String[] updates = {topic};
        try {
            configService.updateLionList(ParamsKey.LionNode.BLACKLIST, op, updates);
        } catch (HttpException e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(e).build();
        }
        return Response.ok().build();
    }
    
    @GET
    @Path("/preassign/{broker}")
    @Produces({MediaType.APPLICATION_JSON})
    public List<String> listPreassignment(
            @PathParam("broker") String broker) {
        LOG.debug("GET: list " + broker + "'s Preassignment");
        List<String> streamIdString = new ArrayList<String>();
        Map<StreamId, String> brokerPressignmentMap = configService.getBrokerPreassignmentCopy();
        for (Map.Entry<StreamId, String> entry : brokerPressignmentMap.entrySet()) {
            if (entry.getValue().equals(broker)) {
                streamIdString.add(entry.getKey().toString());
            }
        }
        return streamIdString;
    }

    @GET
    @Path("/preassign/{topic}/{partition}")
    @Produces({MediaType.APPLICATION_JSON})
    public String getBrokerPreassignmentByPartition(
            @PathParam("topic") String topic,
            @PathParam("partition") String partition) {
        LOG.debug("GET: get broker preassignment by " + topic + "@" + partition);
        return configService.getBrokerPreassignmentByPartition(topic, partition);
    }
}
