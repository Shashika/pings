package edu.colostate.cnrl.sim;

import org.neo4j.graphdb.*;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.*;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

public class SimilarityMeasure {

    @Context
    public GraphDatabaseAPI db;

    @Context
    public Log log;

    private static double redFlagMultiple = 0;
    private static Label queryLabel = null;
    private static Label queryFocusLabel = null;
    private static List<Config> configList = null;
    private static Map<String, String> properties = null;

    @Procedure("cnrl.similarityMeasure")
    public Stream<NodeListResult> similarityMeasure(@Name("similarityScore") double similarityScore,
                                                    @Name("redFlagMultiple") double redFlagMultiple,
                                                    @Name("queryLabel") String queryLabel,
                                                    @Name("queryForcusLabel") String queryFocusLabel){

        this.redFlagMultiple = redFlagMultiple;
        this.queryLabel = Label.label(queryLabel);
        this.queryFocusLabel = Label.label(queryFocusLabel);
        this.configList = readConfigurations();
        this.properties = readProperties(this.configList);

        QueryGraphResult queryGraph = initializeQueryGraph();

        List<List<Node>> resultSet = graphSimilarityMeasure(queryGraph, similarityScore, redFlagMultiple, configList);

        return resultSet.stream().map(nodeList -> new NodeListResult(nodeList));
//        return queryGraph.getAllNodes().stream().map(node -> new NodeResult(node));
    }

    private List<Config> readConfigurations() {

        String line = "";
        String cvsSplitBy = ",";

        InputStream in = getClass().getResourceAsStream("/conf.csv");
        BufferedReader br = new BufferedReader(new InputStreamReader(in));

        List<Config> configList = new ArrayList<>();

        try {
            while ((line = br.readLine()) != null) {

                String[] configs = line.split(cvsSplitBy);
                Config config = new Config();
                config.setType(configs[0]);
                config.setLabel(configs[1]);
                config.setProperty(configs[2]);
                configList.add(config);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return configList;
    }

    private Map<String, String> readProperties(List<Config> configList) {

        Map<String, String> propertyList = new HashMap<>();

        for(Config config : configList){
            if(config.getType().equals("property")){
                propertyList.put(config.getLabel(), config.getProperty());
            }
        }

        return propertyList;
    }

    private List<List<Node>> graphSimilarityMeasure(QueryGraphResult queryGraphResult, double similarityScore,
                                                    double redFlagMultiple, List<Config> configList) {

        List<Node> queryGraph = queryGraphResult.getAllNodes();

        List<List<Node>> matchedGraphs = new ArrayList<>();

        //get all query focus set
        ResourceIterator<Node> qfNodes = db.findNodes(queryFocusLabel);
        double totalWeight = getTotalWeight(queryGraph, redFlagMultiple);

        int limit = 0;
        while (qfNodes.hasNext()) {
            Node qfNode = qfNodes.next();
            MatchedGraph matchedGraph = searchSimilarGraphs(qfNode, queryGraph, configList);

            if(matchedGraph!= null){

                double score = matchedGraph.getMatchScore()/totalWeight;

                if(score >= similarityScore){
                    matchedGraphs.add(matchedGraph.getAllNodes());
                }
//                else{
//                    List<Node> neighbourNodes = searchForLinkedNeighbourNodes(matchedGraph.getAllNodes());
//                    List<List<Node>> neighbourMatchedGraphs = getNeighbourMatchedGraphs(neighbourNodes, queryGraphResult, configList, matchedGraph, similarityScore);
//                    if(neighbourMatchedGraphs != null){
//
//                            matchedGraphs.addAll(neighbourMatchedGraphs);
//
//                    }
//                }
            }
        }
        return matchedGraphs;
    }

    private Label getQueryFocusLabel(List<Node> queryGraph) {
        Label queryFocusLabel = null;

        //Get all queryFocus nodes
        Iterable<Label> iteratorLabel = queryGraph.get(0).getLabels(); //Assume first query node is the query focus node

        //Assume there is only one label in query focus node
        for(Label label : iteratorLabel){
            queryFocusLabel = label;
        }
        return queryFocusLabel;
    }

    private MatchedGraph searchSimilarGraphs(Node matchingRootNode, List<Node> queryGraph, List<Config> configList) {

        MatchedGraph matchedGraph = new MatchedGraph();
        List<Node> allMatchedNodes = new LinkedList<>();
        List<Node> activityNodes = new LinkedList<>();

        List<Node> dataGraph = new CopyOnWriteArrayList<>();
        dataGraph.add(matchingRootNode);

        for(int c = 0; c < queryGraph.size(); c++){

            Node queryNode = queryGraph.get(c);
            List<Node> matchedNodeList = new ArrayList<>();

            for (Node dataNode : dataGraph) {

                if (matchNode(queryNode, dataNode, configList, matchedGraph, allMatchedNodes, activityNodes, true)) {

                    Iterator<Relationship> queryRelationships = queryNode.getRelationships(Direction.OUTGOING).iterator();
                    while (queryRelationships.hasNext()) {

                        Relationship queryRelationship = queryRelationships.next();
                        Iterator<Relationship> dataRelationships = dataNode.getRelationships(Direction.OUTGOING).iterator();

                        while (dataRelationships.hasNext()) {
                            Relationship dataRelationship = dataRelationships.next();
                            Node matchedEndNode = matchEdge(queryRelationship, dataRelationship, configList, matchedGraph, allMatchedNodes, activityNodes);

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

    private Node matchEdge(Relationship queryRelationship, Relationship dataRelationship,
                                List<Config> configList, MatchedGraph matchedGraph, List<Node> allMatchedNodes, List<Node> activityNodes) {

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

            isMatched = matchNode(queryEndNode, dataEndNode, configList, matchedGraph, allMatchedNodes, activityNodes, false);
        }

        if(isMatched){
            matchedEndNode = dataEndNode;
        }

        return matchedEndNode;
    }


    private String getNodeLabelName(Node node) {

        //Assume there is only one label in nodes
        Iterable<Label> labels = node.getLabels();
        return labels.iterator().next().name();
    }

    //There are two matches in nodes - label & name
    private boolean matchNode(Node queryNode, Node dataNode, List<Config> configList, MatchedGraph matchedGraph,
                              List<Node> allMatchedNodes, List<Node> activityNodes, boolean isAddtoMatchedNodes) {

        boolean isMatched = false;
        boolean isFoundConfig = false;

        //Assume there is only one label in nodes
        String queryLabel = getNodeLabelName(queryNode);
        String dataLabel = getNodeLabelName(dataNode);

        //get similarity configs
        for(Config con: configList){

            if(con.getType().equals("node") && con.getLabel().equals(queryLabel)){

                isFoundConfig = true;

                if(con.getProperty() == null){
                    //match label only
                    if(dataLabel.toLowerCase().equals(queryLabel.toLowerCase())){
                        isMatched = true;
                    }
                }
                else{
                    String confPropertyName = con.getProperty();
                    String queryPropertyName = (String)queryNode.getProperty(confPropertyName);
                    String dataPropertyName = (String)dataNode.getProperty(confPropertyName);

                    //match both label and property
                    if(dataLabel.toLowerCase().equals(queryLabel.toLowerCase())
                            && dataPropertyName.toLowerCase().equals(queryPropertyName.toLowerCase())){
                        isMatched = true;
                    }
                }
            }

            if(isFoundConfig){
                break;
            }
        }

        if(isAddtoMatchedNodes && isMatched){
            allMatchedNodes.add(dataNode);

            if(getNodeLabelName(dataNode).equals(getPropertyValue("indicator"))){
                activityNodes.add(dataNode);
                matchedGraph.setMatchScore(matchedGraph.getMatchScore() + getMatchedNodeWeight(dataNode));
            }
        }

        return isMatched;
    }

    private double getTotalWeight(List<Node> graph, double redFlagMultiple) {

        double matchScore = 0;

        for(Node node : graph){

            //similarity measure calculates only for activity nodes
            if(getNodeLabelName(node).equals(getPropertyValue("indicator"))){

                String nodeType = (String)node.getProperty("type");
                if(nodeType.equals("RF")){
                    matchScore += redFlagMultiple;
                }
                else{
                    matchScore += 1;
                }
            }
        }
        return matchScore;
    }

    private double getMatchedNodeWeight(Node node) {

        String nodeType = (String)node.getProperty("type");
        if(nodeType.equals("RF")){
            return this.redFlagMultiple;
        }
        else{
            return  1;
        }
    }

    /**
     * Find the outgoing neighbors of a node in the Neo4j database,
     */
    private ArrayList<Node> findOutNodeNeighbors(Node node) {//find the neighbors of a node in the Neo4j database

        ArrayList<Node> neighborsOfnode = new ArrayList<>();

        Iterator<Relationship> relationships = node.getRelationships(Direction.OUTGOING).iterator();

        while (relationships.hasNext()) {

            Node neighborNode = relationships.next().getOtherNode(node);
            neighborsOfnode.add(neighborNode);
        }
        return neighborsOfnode;
    }

    private QueryGraphResult initializeQueryGraph() {

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

                if(!relationshipType.name().equals(getPropertyValue("knows"))){
                    Node neighborNode = relationship.getOtherNode(popNode);

                    if(neighborNode.hasLabel(queryLabel)){
                        ((LinkedList<Node>) nodeQueue).push(neighborNode);
                        queryGraph.add(neighborNode);
                        if(getNodeLabelName(neighborNode).equals(getPropertyValue("indicator"))){
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

    private String getPropertyValue(String key) {
        return this.properties.get(key);
    }

    private List<Node> searchForLinkedNeighbourNodes(List<Node> graph) {

        List<Node> qfList = new ArrayList<>();
        Node rootNode = graph.get(0);
        qfList.add(rootNode);

        Iterator<Relationship> relationships = rootNode.getRelationships(Direction.OUTGOING).iterator();

        while (relationships.hasNext()) {

            Relationship relationship = relationships.next();

            if(relationship.getType().name().equals(getPropertyValue("knows"))){
                Node neighborNode = relationship.getOtherNode(rootNode);
                qfList.add(neighborNode);
            }
        }
        return qfList;
    }

    private List<List<Node>> getNeighbourMatchedGraphs(List<Node> neighbourNodes, QueryGraphResult queryResult,
                                           List<Config> configList, MatchedGraph initialMatchedGraph, double similarityScore) {

        int[] initialVotes = updateVotes(queryResult.getActivityNodes(), initialMatchedGraph.getActivityNodes());
        int[] activityVotes = initialVotes;

        //avoid exact matches with single user
        List<List<Node>> eligibleNeighbourGraphList = new LinkedList<>();
        eligibleNeighbourGraphList.add(initialMatchedGraph.getAllNodes());

        for(Node node : neighbourNodes ){

            MatchedGraph matchedGraph = searchSimilarGraphs(node, queryResult.getAllNodes(), configList);
            int nodeVotes[] = updateVotes(queryResult.getActivityNodes(), matchedGraph.getActivityNodes());

            if(checkVoteEligibility(initialVotes, nodeVotes) && !checkIndividualStrength(nodeVotes, similarityScore)){
                activityVotes = applyForVotes(activityVotes, nodeVotes);
                eligibleNeighbourGraphList.add(matchedGraph.getAllNodes());
            }
        }

        if(getTotalVoteScore(activityVotes) >= similarityScore){
            return eligibleNeighbourGraphList;
        }
        else {
            return null;
        }
    }

    private boolean checkIndividualStrength(int[] nodeVotes, double similarityScore) {

        double count = 0;
        for(int i = 0; i < nodeVotes.length; i++){
            if(nodeVotes[i] == 1){
                count += nodeVotes[i];
            }
        }
        double individualScore = count/nodeVotes.length;

        if(individualScore >= similarityScore){
            return true;
        }
        return false;
    }

    private double getTotalVoteScore(int[] activityVotes) {

        double count = 0;

        for(int i = 0; i < activityVotes.length; i++){
            if(activityVotes[i] != 0){
                count += activityVotes[i];
            }
        }
        return count/activityVotes.length;
    }

    private int[] applyForVotes(int[] activityVotes, int[] nodeVotes) {

        for(int i = 0; i < activityVotes.length; i++){
            if(activityVotes[i] == 0 && nodeVotes[i] == 1){
                activityVotes[i] = 1;
            }
        }
        return activityVotes;
    }

    private boolean checkVoteEligibility(int[] initialVotes, int[] nodeVotes) {

        boolean isEligible = false;

        for(int i = 0; i < initialVotes.length; i++){
            if(initialVotes[i] == 0 && nodeVotes[i] == 1){
                isEligible = true;
            }
        }
        return isEligible;
    }

    private int[] updateVotes(List<Node> activityNodes, List<Node> matchedActivityNodes) {

        int[] votes = new int[activityNodes.size()];
        int i = 0;
        for(Node node : activityNodes){
            String nodeName = (String) node.getProperty(getPropertyValue("indicator_match"));

            for(Node matchedNode : matchedActivityNodes){

                String matchedNodeName = (String) matchedNode.getProperty(getPropertyValue("indicator_match"));
                if(nodeName.toLowerCase().equals(matchedNodeName.toLowerCase())){
                    votes[i] = 1;
                    break;
                }
            }
            i++;
        }
        return votes;
    }

}
