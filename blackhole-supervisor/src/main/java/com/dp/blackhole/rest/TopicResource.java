package com.dp.blackhole.rest;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mortbay.util.ajax.JSON;

import com.dp.blackhole.supervisor.model.ConsumerDesc;
import com.dp.blackhole.supervisor.model.ConsumerGroup;
import com.dp.blackhole.supervisor.model.ConsumerGroupKey;
import com.dp.blackhole.supervisor.model.Stage;
import com.dp.blackhole.supervisor.model.Stream;
import com.dp.blackhole.supervisor.model.TopicConfig;

@Path("topics")
public class TopicResource extends BaseResource {
    private static final Log LOG = LogFactory.getLog(TopicResource.class);
    
    @GET
    @Path("/")
    @Produces({MediaType.APPLICATION_JSON})
    public Response getAllTopics() {
        LOG.info("GET: topic names");
        Set<String> topics = supervisorService.getAllTopicNames();
        final String js = JSON.toString(topics.toArray(new String[topics.size()]));
        return Response.ok(js).build();
    }
    
    @GET
    @Path("/{topic}")
    @Produces({MediaType.APPLICATION_JSON})
    public Response getApplicationByTopic(
            @PathParam("topic") final String topic) {
        LOG.info("GET: topic -> app");
        TopicConfig confInfo = configService.getConfByTopic(topic);
        if (confInfo != null) {
            return Response.ok(JSON.toString(confInfo.getAppName())).build();
        } else {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
    }
    
    @GET
    @Path("/{topic}/streams")
    @Produces({MediaType.APPLICATION_JSON})
    public List<Stream> getAllStreamsByTopic(
            @PathParam("topic") final String topic) {
        LOG.info("GET: topic -> streams");
        List<Stream> streams = supervisorService.getAllStreams(topic);
        return streams;
    }
    
    @GET
    @Path("/{topic}/streams/{source}")
    @Produces({MediaType.APPLICATION_JSON})
    public Stream getStreamInfo(
            @PathParam("topic") final String topic,
            @PathParam("source") final String source) {
        LOG.info("GET: topic, source -> stream");
        return supervisorService.getStream(topic, source);
    }
    
    @DELETE
    @Path("/{topic}/streams/{source}")
    @Produces({MediaType.APPLICATION_JSON})
    public Response retireStream(@PathParam("topic") final String topic,
            @PathParam("source") final String source) {
        LOG.info("DELETE: topic, source -> stream");
        supervisorService.retireStream(topic, source);
        return Response.ok().build();
    }
    
    @GET
    @Path("/{topic}/streams/{source}/stages")
    @Produces({MediaType.APPLICATION_JSON})
    public List<Stage> getAllStagesFromStream(
            @PathParam("topic") final String topic,
            @PathParam("source") final String source) {
        LOG.info("GET: topic, source -> stages");
        List<Stage> stages = null;
        Stream stream = supervisorService.getStream(topic, source);
        if (stream != null) {
            stages = new ArrayList<Stage>(stream.getStages());
        }
        return stages;
    }
    
    @GET
    @Path("/{topic}/groups")
    @Produces({MediaType.APPLICATION_JSON})
    public Set<ConsumerGroupKey> getAllConsumerGroup(
            @PathParam("topic") final String topic) {
        LOG.info("GET: topic -> groups");
        SortedSet<ConsumerGroupKey> groupsSorted  = new TreeSet<ConsumerGroupKey>(new Comparator<ConsumerGroupKey>() {
            @Override
            public int compare(ConsumerGroupKey o1, ConsumerGroupKey o2) {
                int topicResult = o1.getTopic().compareTo(o2.getTopic());
                return topicResult == 0 ? o1.getGroupId().compareTo(o2.getGroupId()) : topicResult;
            }
        });
        groupsSorted.addAll(supervisorService.getAllConsumerGroupKeys());
        return groupsSorted;
    }
    
    @GET
    @Path("/{topic}/groups/{groupId}")
    @Produces({MediaType.APPLICATION_JSON})
    public List<ConsumerDesc> getAllConsumersByGroupId(
            @PathParam("topic") final String topic,
            @PathParam("groupId") final String groupId) {
        LOG.info("GET: topic, groupId -> consumer");
        ConsumerGroupKey groupKey = new ConsumerGroupKey(groupId, topic);
        ConsumerGroup group = supervisorService.getConsumerGroup(groupKey);
        return group.getConsumes();
    }
    
    @GET
    @Path("/{topic}/groups/{groupId}/commit")
    @Produces({MediaType.APPLICATION_JSON})
    public Map<String, AtomicLong> getAllCommittedOffsets(
            @PathParam("topic") final String topic,
            @PathParam("groupId") final String groupId) {
        LOG.info("GET: topic, groupId -> conmitted offsets");
        ConsumerGroupKey groupKey = new ConsumerGroupKey(groupId, topic);
        ConsumerGroup group = supervisorService.getConsumerGroup(groupKey);
        return group.getCommitedOffsets();
    }
    
    @GET
    @Path("/detail/groups")
    @Produces({MediaType.APPLICATION_JSON})
    public Set<ConsumerGroup> getAllConsumerGroupDetail(
            @PathParam("topic") final String topic) {
        LOG.info("GET: topic -> groups detail");
        return supervisorService.getCopyOfConsumerGroups();
    }
}
