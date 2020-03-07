package edu.colostate.cnrl.sim;

import org.neo4j.graphdb.*;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.*;
import java.util.stream.Stream;

public class NeighborhoodMeasure {

    @Context
    public GraphDatabaseAPI db;

    @Context
    public Log log;

    private static double redFlagMultiple = 0;
    private static Label queryLabel = null;
    private static Label queryFocusLabel = null;
    private static Map<String, List<String>> configList = null;
    private static String NeighbourRelType = "NeighbourRelType";
    private static String ActivityNodeType = "ActivityNodeType";
    private static String RedFlag = "RedFlag";
    private static String configFileName = "/conf_rad.json";

    @Procedure("cnrl.neighborhoodMeasure")
    public Stream<NodeListResult> neighborhoodMeasure(@Name("similarityScore") double similarityScore,
                                                    @Name("redFlagMultiple") double redFlagMultiple,
                                                    @Name("queryLabel") String queryLabel,
                                                    @Name("queryForcusLabel") String queryFocusLabel){

        this.redFlagMultiple = redFlagMultiple;
        this.queryLabel = Label.label(queryLabel);
        this.queryFocusLabel = Label.label(queryFocusLabel);

        Common common = new Common(this.db, configList, this.queryLabel, this.queryFocusLabel, NeighbourRelType,
                ActivityNodeType, this.redFlagMultiple, RedFlag, configFileName);

        this.configList = common.readConfigurations();

        QueryGraphResult queryGraph = common.initializeQueryGraph();

        List<List<Node>> resultSet = graphNeighborhoodMeasure(common, queryGraph, similarityScore, redFlagMultiple);

        return resultSet.stream().map(nodeList -> new NodeListResult(nodeList));
//        return queryGraph.getAllNodes().stream().map(node -> new NodeResult(node));
    }


    private List<List<Node>> graphNeighborhoodMeasure(Common common, QueryGraphResult queryGraphResult, double similarityScore, double redFlagMultiple) {

        List<Node> queryGraph = queryGraphResult.getAllNodes();

        List<List<Node>> matchedGraphs = new ArrayList<>();

        //get all query focus set
        ResourceIterator<Node> qfNodes = db.findNodes(queryFocusLabel);

        while (qfNodes.hasNext()) {
            Node qfNode = qfNodes.next();
            MatchedGraph matchedGraph = common.searchSimilarGraphs(common, qfNode, queryGraph);

            if(matchedGraph!= null){

                List<Node> neighbourNodes = searchForLinkedNeighbourNodes(matchedGraph.getAllNodes());
                List<List<Node>> neighbourMatchedGraphs = getNeighbourMatchedGraphs(common, neighbourNodes, queryGraphResult, matchedGraph, similarityScore);
                if(neighbourMatchedGraphs != null){
                    matchedGraphs.addAll(neighbourMatchedGraphs);
                }
            }
        }
        return matchedGraphs;
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

    private List<Node> searchForLinkedNeighbourNodes(List<Node> graph) {

        List<Node> qfList = new ArrayList<>();
        Node rootNode = graph.get(0);
        qfList.add(rootNode);

        Iterator<Relationship> relationships = rootNode.getRelationships(Direction.OUTGOING).iterator();

        while (relationships.hasNext()) {

            Relationship relationship = relationships.next();

            if(relationship.getType().name().equals(configList.get(NeighbourRelType).get(0))){
                Node neighborNode = relationship.getOtherNode(rootNode);
                qfList.add(neighborNode);
            }
        }
        return qfList;
    }

    private List<List<Node>> getNeighbourMatchedGraphs(Common common, List<Node> neighbourNodes, QueryGraphResult queryResult,
                                                       MatchedGraph initialMatchedGraph, double similarityScore) {

        int[] initialVotes = updateVotes(queryResult.getActivityNodes(), initialMatchedGraph.getActivityNodes());
        int[] activityVotes = initialVotes;

        //avoid exact matches with single user
        List<List<Node>> eligibleNeighbourGraphList = new LinkedList<>();
        eligibleNeighbourGraphList.add(initialMatchedGraph.getAllNodes());

        for(Node node : neighbourNodes ){

            MatchedGraph matchedGraph = common.searchSimilarGraphs(common, node, queryResult.getAllNodes());
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
            // Assume only one attribute to match
            String nodeName = (String) node.getProperty(configList.get(configList.get(ActivityNodeType).get(0)).get(0));

            for(Node matchedNode : matchedActivityNodes){

                String matchedNodeName = (String) matchedNode.getProperty(configList.get(configList.get(ActivityNodeType).get(0)).get(0));
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
