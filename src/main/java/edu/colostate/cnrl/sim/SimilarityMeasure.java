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

    @Procedure("cnrl.similarityMeasure")
    public Stream<NodeListResult> similarityMeasure(@Name("similarityScore") double similarityScore,
                                                    @Name("redFlagMultiple") double redFlagMultiple){

        List<Node> queryGraph = initializeQueryGraph();
        List<Config> configList = readConfigurations();

        List<List<Node>> resultSet = graphSimilarityMeasure(queryGraph, similarityScore, redFlagMultiple, configList);

        return resultSet.stream().map(nodeList -> new NodeListResult(nodeList));
//        return queryGraph.stream().map(node -> new NodeResult(node));
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

    private List<List<Node>> graphSimilarityMeasure(List<Node> queryGraph, double similarityScore,
                                                    double redFlagMultiple, List<Config> configList) {

        Label queryFocusLabel = getQueryFocusLabel(queryGraph);

        List<List<Node>> matchedGraphs = new ArrayList<>();

        //get all query focus set
        ResourceIterator<Node> qfNodes = db.findNodes(queryFocusLabel);
        double totalWeight = getTotalWeight(queryGraph, redFlagMultiple);

        while (qfNodes.hasNext()) {
            Node qfNode = qfNodes.next();
            List<Node> matchedGraph = searchNeighbourNodes(qfNode, queryGraph, redFlagMultiple, configList);

            if(matchedGraph!= null){

                double score = getTotalWeight(matchedGraph, redFlagMultiple)/totalWeight;
//                double score = (double) matchedGraph.getMatchedGraph().size()/queryGraph.size();

                if(score >= similarityScore){
                    matchedGraphs.add(matchedGraph);
                }
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

    private List<Node> searchNeighbourNodes(Node matchingRootNode, List<Node> queryGraph,
                                              double redFlagMultiple, List<Config> configList) {

        List<Node> matchedGraph = new LinkedList<>();
        List<Node> dataGraph = new CopyOnWriteArrayList<>();
        dataGraph.add(matchingRootNode);

        for(int c = 0; c < queryGraph.size(); c++){

            Node queryNode = queryGraph.get(c);
            List<Node> matchedNodeList = new ArrayList<>();

            for (Node dataNode : dataGraph) {

                if (matchNode(queryNode, dataNode, configList, matchedGraph, true)) {

                    //remove matched node
                    dataGraph.remove(dataNode);

                    Iterator<Relationship> queryRelationships = queryNode.getRelationships(Direction.OUTGOING).iterator();
                    while (queryRelationships.hasNext()) {

                        Relationship queryRelationship = queryRelationships.next();
                        Iterator<Relationship> dataRelationships = dataNode.getRelationships(Direction.OUTGOING).iterator();

                        while (dataRelationships.hasNext()) {
                            Relationship dataRelationship = dataRelationships.next();
                            Node matchedEndNode = matchEdge(queryRelationship, dataRelationship, configList, matchedGraph);

                            if(matchedEndNode != null && !isMatchedNodeAlreadyInList(matchedNodeList, matchedEndNode)){
                                matchedNodeList.add(matchedEndNode);
                            }
                        }
                    }
                }
            }
            addNewMatchedListToDataGraph(dataGraph, matchedNodeList);
        }
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
                                List<Config> configList, List<Node> matchedGraph) {

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

            isMatched = matchNode(queryEndNode, dataEndNode, configList, matchedGraph, false);
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
    private boolean matchNode(Node queryNode, Node dataNode, List<Config> configList,
                              List<Node> matchedGraph, boolean isAddtoMatchedGraph) {

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

        if(isAddtoMatchedGraph && isMatched){
            matchedGraph.add(dataNode);
        }

        return isMatched;
    }

    private double getTotalWeight(List<Node> graph, double redFlagMultiple) {

        double matchScore = 0;

        for(Node node : graph){

            //similarity measure calculates only for activity nodes
            if(getNodeLabelName(node).equals("Blog")){

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

    private List<Node> initializeQueryGraph() {

        long userId = 353;

        /*get root node*/
        List<Node> queryGraph = new LinkedList<>();
        Node qf =  db.getNodeById(userId);

        NodeDetail rootNodeDetail = new NodeDetail(qf);
        queryGraph.add(qf);

        List<NodeDetail> nodeQueue = new LinkedList<>();
        ((LinkedList<NodeDetail>) nodeQueue).push(rootNodeDetail);

        while(nodeQueue.size() != 0){

            NodeDetail popNodeDetail = ((LinkedList<NodeDetail>) nodeQueue).pop();
            Iterator<Relationship> relationships = popNodeDetail.getNode().getRelationships(Direction.OUTGOING).iterator();

            while (relationships.hasNext()) {

                //node has other out edges - not a leaf node
                popNodeDetail.setNotLeafNode(true);

                Relationship relationship = relationships.next();
                RelationshipType relationshipType = relationship.getType();

                if(!relationshipType.name().equals("FRIENDS")){ //avoid FRIENDS relationships
                    Node neighborNode = relationship.getOtherNode(popNodeDetail.getNode());
                    NodeDetail neighborNodeDetail = new NodeDetail(neighborNode);

                    ((LinkedList<NodeDetail>) nodeQueue).push(neighborNodeDetail);
                    queryGraph.add(neighborNode);

                }

            }
        }

        return queryGraph;
    }

}
