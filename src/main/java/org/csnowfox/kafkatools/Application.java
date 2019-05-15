package org.csnowfox.kafkatools;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.validators.PositiveInteger;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.concurrent.ExecutionException;

/**
 * @ClassName: Application
 * @Description Kafka console tool entry
 * @Author Csnowfox
 **/
@Parameters(separators = "=")
public class Application {

    @Parameter(names = "--help", help = true)
    private boolean help;

    @Parameter(names = "--broker", required = true, description = "broker url, for example --broker=192.168.19.61:9092", help=true, order = 1)
    private String broker;

    @Parameter(names = "--topic-list", description = "list all topics", help = true, order = 2)
    private boolean topicList;

    @Parameter(names = "--topic-list-offset", description = "list all topics with offset", help = true, order = 2)
    private String topicListOffset;

    @Parameter(names = "--topic-create", description ="create a topic, for example --topic-create=topicName -partitions=3 -replication=2", help = true, order = 2)
    private String topicCreate;

    @Parameter(names = "-partitions", description ="for command --topic-create", order = 3, hidden = true, validateWith = PositiveInteger.class)
    private int partitions;

    @Parameter(names = "-replication", description ="for command --topic-create", order = 4, hidden = true, validateWith = PositiveInteger.class)
    private short replication;

    @Parameter(names = "--topic-delete", description ="delete a topic, for example --topic-delete=topicName", help = true, order = 5)
    private String topicDelete;


    @Parameter(names = "--group", description = "list consumer group information, for example --group=group1 [-topic=topicName]", help = true, order = 6)
    private String group;

    @Parameter(names = "-topic", description = "or command --group", help = true, order = 6, hidden = true)
    private String topic;


    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Application app = new Application();
        JCommander j = JCommander.newBuilder().addObject(app).build();
        j.parse(args);
        app.run(j);
    }

    public void run(JCommander j) throws ExecutionException, InterruptedException {

        if (help) {
            j.setProgramName("java -jar kafkatools.jar");
            j.usage();
            return;
        }

        if (broker == null || broker.trim().equals("")) {
            JCommander.getConsole().println("ERROR: Broker is a required parameter");
            j.usage();
            return;
        }

        Tools tools = new Tools(broker);
        if (topicList) {
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            JCommander.getConsole().println(gson.toJson(tools.getTopicList()));
            return;
        }

        if (topicListOffset != null && !topicListOffset.trim().equals("")) {
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            JCommander.getConsole().println(gson.toJson(tools.getTopicOffset(topicListOffset)));
            return;
        }

        if (topicCreate != null && !topicCreate.trim().equals("")) {
            tools.createTopic(topicCreate, partitions, replication);
            return;
        }

        if (topicDelete != null && !topicDelete.trim().equals("")) {
            tools.deleteTopic(topicDelete);
            return;
        }

        if (group != null && !group.trim().equals("")) {
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            JCommander.getConsole().println(gson.toJson(tools.consumerGroupListing(group, topic)));
            return;
        }

    }

}
