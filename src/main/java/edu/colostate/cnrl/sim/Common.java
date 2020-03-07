package edu.colostate.cnrl.sim;

import com.google.gson.Gson;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

class Common {

    private GraphDatabaseAPI db;
    private Map<String, List<String>> configList;
    private Label queryLabel;
    private Label queryFocusLabel;
    private String NeighbourRelType;
    private String ActivityNodeType;
    private double redFlagMultiple;
    private String RedFlag;
    private String configFileName;

    public Common(GraphDatabaseAPI db, Map<String, List<String>> configList, Label queryLabel, Label queryFocusLabel,
                  String NeighbourRelType, String ActivityNodeType, double redFlagMultiple, String RedFlag, String configFileName) {
        this.db = db;
        this.configList = configList;
        this.queryLabel = queryLabel;
        this.queryFocusLabel = queryFocusLabel;
        this.NeighbourRelType = NeighbourRelType;
        this.ActivityNodeType = ActivityNodeType;
        this.redFlagMultiple = redFlagMultiple;
        this.RedFlag = RedFlag;
        this.configFileName = configFileName;
    }

    public Map<String, List<String>> readConfigurations() {

        Conf conf = null;
        Map<String, List<String>> configs = new HashMap<>();
        InputStream in = getClass().getResourceAsStream(configFileName);
        BufferedReader streamReader = null;
        StringBuilder responseStrBuilder = null;
        try {
            streamReader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            responseStrBuilder = new StringBuilder();
            String inputStr;
            while (true) {
                if ((inputStr = streamReader.readLine()) == null) break;
                responseStrBuilder.append(inputStr);
            }
            conf = new Gson().fromJson(responseStrBuilder.toString(), Conf.class);
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (NodeDetails d : conf.getNodes()) {

            List<String> propertiesList = null;

            for (String attr : d.getAttrs()) {
                if (propertiesList == null) {
                    propertiesList = new ArrayList<>();
                }
                propertiesList.add(attr);
            }
            configs.put(d.getLabel(), propertiesList);
        }
        configs.put(RedFlag, conf.getRed_flag().getAttrs());

        List<String> relType = new ArrayList<>();
        relType.add(conf.getNeighbour_relationship_type());

        List<String> nodeType = new ArrayList<>();
        nodeType.add(conf.getActivity_node_type());

        configs.put(NeighbourRelType, relType);
        configs.put(ActivityNodeType, nodeType);

        return configs;
    }


    public MatchedGraph searchSimilarGraphs(Common common, Node matchingRootNode, List<Node> queryGraph) {

        MatchedGraph matchedGraph = new MatchedGraph();
        List<Node> allMatchedNodes = new LinkedList<>();
        List<Node> activityNodes = new LinkedList<>();

        List<Node> dataGraph = new CopyOnWriteArrayList<>();
        dataGraph.add(matchingRootNode);

        for(int c = 0; c < queryGraph.size(); c++){

            Node queryNode = queryGraph.get(c);
            List<Node> matchedNodeList = new ArrayList<>();

            for (Node dataNode : dataGraph) {

                if (common.matchNode(queryNode, dataNode, matchedGraph, allMatchedNodes, activityNodes, true)) {

                    Iterator<Relationship> queryRelationships = queryNode.getRelationships(Direction.OUTGOING).iterator();
                    while (queryRelationships.hasNext()) {

                        Relationship queryRelationship = queryRelationships.next();
                        Iterator<Relationship> dataRelationships = dataNode.getRelationships(Direction.OUTGOING).iterator();

                        while (dataRelationships.hasNext()) {
                            Relationship dataRelationship = dataRelationships.next();
                            Node matchedEndNode = common.matchEdge(queryRelationship, dataRelationship, matchedGraph, allMatchedNodes, activityNodes);

                            if(matchedEndNode != null && !isMatchedNodeAlreadyInList(matchedNodeList, matchedEndNode)){
                                matchedNodeList.add(matchedEndNode);
                            }
                        }
                    }
                    //remove matched node
                    dataGraph.remove(dataNode);
                }
            }
            addNewMatchedListToDataGraph(dataGraph, matchedNodeList);
        }
        matchedGraph.setAllNodes(allMatchedNodes);
        matchedGraph.setActivityNodes(activityNodes);

        return matchedGraph;
    }

    //A node contain on matched list only one time
    private boolean isMatchedNodeAlreadyInList(List<Node> matchedNodeList, Node node) {

        boolean isNodeAlreadyInList = false;

        for(Node matchedNode : matchedNodeList){
            if(matchedNode.getId() == node.getId()){
                isNodeAlreadyInList = true;
                break;
            }
        }
        return isNodeAlreadyInList;
    }

    private void addNewMatchedListToDataGraph(List<Node> dataGraph, List<Node> matchedNodeList) {

        for (Node node : matchedNodeList) {
            dataGraph.add(node);
        }
    }

    public Node matchEdge(Relationship queryRelationship, Relationship dataRelationship,
                           MatchedGraph matchedGraph, List<Node> allMatchedNodes, List<Node> activityNodes) {

        RelationshipType queryRelationshipType = queryRelationship.getType();
        RelationshipType dataRelationshipType = dataRelationship.getType();

        boolean isMatched = false;
        Node matchedEndNode = null;
        Node dataEndNode = null;

        //matchRelation
        if(queryRelationshipType.name().toLowerCase().equals(dataRelationshipType.name().toLowerCase())){

            //match end nodes
            Node queryEndNode = queryRelationship.getEndNode();
            dataEndNode = dataRelationship.getEndNode();

            isMatched = matchNode(queryEndNode, dataEndNode, matchedGraph, allMatchedNodes, activityNodes, false);
        }

        if(isMatched){
            matchedEndNode = dataEndNode;
        }

        return matchedEndNode;
    }

    //There are two matches in nodes - label & attrs
    public boolean matchNode(Node queryNode, Node dataNode, MatchedGraph matchedGraph,
                              List<Node> allMatchedNodes, List<Node> activityNodes, boolean isAddtoMatchedNodes) {

        boolean isMatched = false;

        //Assume there is only one label in nodes
        String queryLabel = getNodeLabelName(queryNode);
        String dataLabel = getNodeLabelName(dataNode);

        if(queryLabel.equals(dataLabel)){
            //get similarity configs
            List<String> attrs = configList.get(dataLabel);

            if(attrs == null){
                isMatched = true;
            }
            else {
                boolean isAttrsMatched = true;
                for(String a: attrs){
                    String queryPropertyName = (String)queryNode.getProperty(a);
                    String dataPropertyName = (String)dataNode.getProperty(a);

                    if(!queryPropertyName.toLowerCase().equals(dataPropertyName.toLowerCase())) {
                        isAttrsMatched = false;
                    }
                }
                if(isAttrsMatched){
                    isMatched = true;
                }
            }
        }

        if(isAddtoMatchedNodes && isMatched){
            allMatchedNodes.add(dataNode);

            if(getNodeLabelName(dataNode).equals(configList.get(ActivityNodeType).get(0))){
                activityNodes.add(dataNode);
                matchedGraph.setMatchScore(matchedGraph.getMatchScore() + getMatchedNodeWeight(dataNode));
            }
        }

        return isMatched;
    }

    private String getNodeLabelName(Node node) {

        //Assume there is only one label in nodes
        Iterable<Label> labels = node.getLabels();
        return labels.iterator().next().name();
    }

    public QueryGraphResult initializeQueryGraph() {

        QueryGraphResult results = new QueryGraphResult();

        ResourceIterator<Node> qfNodes = db.findNodes(queryFocusLabel);

        List<Node> queryGraph = new LinkedList<>();
        List<Node> activityNodesGraph = new LinkedList<>();

        Node qf = null;
        /*get root node*/
        while (qfNodes.hasNext()) {
            Node qfNode = qfNodes.next();
            if(qfNode.hasLabel(queryLabel)){
                qf = qfNode;
                queryGraph.add(qfNode);
                break;
            }
        }

        List<Node> nodeQueue = new LinkedList<>();
        ((LinkedList<Node>) nodeQueue).push(qf);

        while(nodeQueue.size() != 0){

            Node popNode = ((LinkedList<Node>) nodeQueue).pop();
            Iterator<Relationship> relationships = popNode.getRelationships(Direction.OUTGOING).iterator();

            while (relationships.hasNext()) {

                Relationship relationship = relationships.next();
                RelationshipType relationshipType = relationship.getType();

                if(!relationshipType.name().equals(configList.get(NeighbourRelType).get(0))){
                    Node neighborNode = relationship.getOtherNode(popNode);

                    if(neighborNode.hasLabel(queryLabel)){
                        ((LinkedList<Node>) nodeQueue).push(neighborNode);
                        queryGraph.add(neighborNode);
                        if(getNodeLabelName(neighborNode).equals(configList.get(ActivityNodeType).get(0))){
                            activityNodesGraph.add(neighborNode);
                        }
                    }
                }
            }
        }
        results.setAllNodes(queryGraph);
        results.setActivityNodes(activityNodesGraph);

        return results;
    }

    private double getMatchedNodeWeight(Node node) {

        String nodeType = (String)node.getProperty(configList.get(RedFlag).get(0));
        if(nodeType.equals(configList.get(RedFlag).get(1))){
            return this.redFlagMultiple;
        }
        else{
            return  1;
        }
    }

    public double getTotalWeight(List<Node> graph, double redFlagMultiple) {

        double matchScore = 0;

        for(Node node : graph){

            //similarity measure calculates only for activity nodes
            if(getNodeLabelName(node).equals(configList.get(ActivityNodeType).get(0))){

                String nodeType = (String)node.getProperty(configList.get(RedFlag).get(0));
                if(nodeType.equals(configList.get(RedFlag).get(1))){
                    matchScore += redFlagMultiple;
                }
                else{
                    matchScore += 1;
                }
            }
        }
        return matchScore;
    }
}
