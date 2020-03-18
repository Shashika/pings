package edu.colostate.cnrl.sim;

import com.google.gson.Gson;
import org.neo4j.cypher.internal.frontend.v2_3.ast.functions.Str;
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
    private String IdentifierType;
    private double redFlagMultiple;
    private String RedFlag;
    private String configFileName;
    private String entity;
    private String identifier;
    private String identifierName;
    private String relationShipToFocus;
    private boolean isOutgoingDirFocusToAct = false;

    public Common(GraphDatabaseAPI db, Label queryLabel, Label queryFocusLabel,
                  String NeighbourRelType, String ActivityNodeType, double redFlagMultiple, String RedFlag, String configFileName) {
        this.db = db;
        this.queryLabel = queryLabel;
        this.queryFocusLabel = queryFocusLabel;
        this.NeighbourRelType = NeighbourRelType;
        this.ActivityNodeType = ActivityNodeType;
        this.redFlagMultiple = redFlagMultiple;
        this.RedFlag = RedFlag;
        this.configFileName = configFileName;
    }

    public Common(GraphDatabaseAPI db, String entity, String identifier, String identifierName, String relationShipToFocus,
                  String NeighbourRelType, String ActivityNodeType, String IdentifierType, double redFlagMultiple, String RedFlag, String configFileName) {
        this.db = db;
        this.entity = entity;
        this.identifier = identifier;
        this.identifierName = identifierName;
        this.relationShipToFocus = relationShipToFocus;
        this.NeighbourRelType = NeighbourRelType;
        this.ActivityNodeType = ActivityNodeType;
        this.IdentifierType = IdentifierType;
        this.redFlagMultiple = redFlagMultiple;
        this.RedFlag = RedFlag;
        this.configFileName = configFileName;
    }

    public Map<String, List<String>> readConfigurations() {

        Conf conf = null;
        Map<String, List<String>> configs = new HashMap<>();
        InputStream in = getClass().getResourceAsStream(configFileName);
        BufferedReader streamReader;
        StringBuilder responseStrBuilder;
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

        List<String> identifierType = new ArrayList<>();
        identifierType.add(conf.getIdentifier_type());

        configs.put(NeighbourRelType, relType);
        configs.put(ActivityNodeType, nodeType);
        configs.put(IdentifierType, identifierType);

        return configs;
    }


    public MatchedGraph searchSimilarGraphs(Node matchingRootNode, List<Node> queryGraph) {

        MatchedGraph matchedGraph = new MatchedGraph();
        List<Node> allMatchedNodes = new LinkedList<>();
        List<Node> activityNodes = new LinkedList<>();

        List<Node> dataGraph = new CopyOnWriteArrayList<>();
        dataGraph.add(matchingRootNode);

        for(int c = 0; c < queryGraph.size(); c++){

            Node queryNode = queryGraph.get(c);
            List<Node> matchedNodeList = new ArrayList<>();

            for (Node dataNode : dataGraph) {

                if (matchNode(queryNode, dataNode, matchedGraph, allMatchedNodes, activityNodes, true)) {

                    Iterator<Relationship> queryRelationships = queryNode.getRelationships(Direction.OUTGOING).iterator();
                    while (queryRelationships.hasNext()) {

                        Relationship queryRelationship = queryRelationships.next();
                        Iterator<Relationship> dataRelationships = dataNode.getRelationships(Direction.OUTGOING).iterator();

                        while (dataRelationships.hasNext()) {
                            Relationship dataRelationship = dataRelationships.next();
                            Node matchedEndNode = matchEdge(queryRelationship, dataRelationship, matchedGraph, allMatchedNodes, activityNodes, true);

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

    public MatchedGraph searchImplicitSimilarGraphs(Node matchingRootNode, List<Node> queryGraph) {

        MatchedGraph matchedGraph = new MatchedGraph();
        List<Node> allMatchedNodes = new LinkedList<>();
        List<Node> activityNodes = new LinkedList<>();

        List<Node> dataGraph = new CopyOnWriteArrayList<>();
        dataGraph.add(matchingRootNode);

        for(int c = 0; c < queryGraph.size(); c++){

            Node queryNode = queryGraph.get(c);
            List<Node> matchedNodeList = new ArrayList<>();

            for (Node dataNode : dataGraph) {

                if (matchNode(queryNode, dataNode, matchedGraph, allMatchedNodes, activityNodes, true)) {

                    Iterator<Relationship> queryRelationships;
                    if(isOutgoingDirFocusToAct)
                        queryRelationships = queryNode.getRelationships(Direction.OUTGOING).iterator();
                    else
                        queryRelationships = queryNode.getRelationships(Direction.INCOMING).iterator();

                    while (queryRelationships.hasNext()) {

                        Relationship queryRelationship = queryRelationships.next();

                        Iterator<Relationship> dataRelationships;
                        if(isOutgoingDirFocusToAct)
                            dataRelationships = dataNode.getRelationships(Direction.OUTGOING).iterator();
                        else
                            dataRelationships = dataNode.getRelationships(Direction.INCOMING).iterator();


                        while (dataRelationships.hasNext()) {
                            Relationship dataRelationship = dataRelationships.next();

                            Node matchedEndNode = matchEdge(queryRelationship, dataRelationship, matchedGraph, allMatchedNodes, activityNodes, isOutgoingDirFocusToAct);

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
                           MatchedGraph matchedGraph, List<Node> allMatchedNodes, List<Node> activityNodes, boolean isOutgoingRel) {

        RelationshipType queryRelationshipType = queryRelationship.getType();
        RelationshipType dataRelationshipType = dataRelationship.getType();

        boolean isMatched = false;
        Node matchedEndNode = null;
        Node otherDataNode = null;
        Node otherQueryNode;

        //matchRelation
        if(queryRelationshipType.name().toLowerCase().equals(dataRelationshipType.name().toLowerCase())){

            //match end nodes
            if(isOutgoingRel){
                otherQueryNode = queryRelationship.getEndNode();
                otherDataNode = dataRelationship.getEndNode();
            }
            else{
                otherQueryNode = queryRelationship.getStartNode();
                otherDataNode = dataRelationship.getStartNode();
            }


            isMatched = matchNode(otherQueryNode, otherDataNode, matchedGraph, allMatchedNodes, activityNodes, false);
        }

        if(isMatched){
            matchedEndNode = otherDataNode;
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
                //In this version, (PINGS 1.0), we assume that there is only one property is considered for similarity.
                for(String a: attrs){
                    String queryPropertyName = (String)queryNode.getProperty(a);
                    //Due to the inconsistency of datasets
                    String dataPropertyName = null;
                    if(dataNode.hasProperty(a)) {
                        dataPropertyName = (String) dataNode.getProperty(a);
                    }

                    if(dataPropertyName == null || !queryPropertyName.toLowerCase().equals(dataPropertyName.toLowerCase())) {
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

    public QueryGraphResult initializeImplicitQueryGraph() {

        QueryGraphResult results = new QueryGraphResult();

        List<Node> nodeList = new ArrayList<>();

        Node focusNode;
        if(this.identifier.equals(configList.get(IdentifierType).get(0))){
            focusNode = db.getNodeById(Long.valueOf(this.identifierName));
        }
        else {
            focusNode = db.findNode(Label.label(this.entity), this.identifier, this.identifierName);
        }
        nodeList.add(focusNode);

        Iterator<Relationship> relationships = focusNode.getRelationships(Direction.BOTH).iterator();
        while (relationships.hasNext()) {

            Relationship relationship = relationships.next();
            RelationshipType relationshipType = relationship.getType();
            if(relationshipType.name().equals(this.relationShipToFocus)){

                //check direction (focus to activity), Assume there is only one direction
                isOutgoingDirFocusToAct = focusNode.hasRelationship(Direction.OUTGOING, RelationshipType.withName(this.relationShipToFocus));

                Node activityNode = relationship.getOtherNode(focusNode);
                nodeList.add(activityNode);
            }
        }
        results.setAllNodes(nodeList);
        return results;
    }

    private double getMatchedNodeWeight(Node node) {

        String nodeType;
        if(configList.get(RedFlag).size()>0){
            nodeType = (String)node.getProperty(configList.get(RedFlag).get(0));

            if(nodeType.equals(configList.get(RedFlag).get(1))){
                return this.redFlagMultiple;
            }
        }
        return  1;
    }

    public double getTotalWeight(List<Node> graph, double redFlagMultiple) {

        double matchScore = 0;

        for(Node node : graph){

            //similarity measure calculates only for activity nodes
            if(getNodeLabelName(node).equals(configList.get(ActivityNodeType).get(0))){

                String nodeType = null;
                if(configList.get(RedFlag).size() > 0){
                    nodeType = (String)node.getProperty(configList.get(RedFlag).get(0));
                }

                if(nodeType != null && nodeType.equals(configList.get(RedFlag).get(1))){
                    matchScore += redFlagMultiple;
                }
                else{
                    matchScore += 1;
                }
            }
        }
        return matchScore;
    }

    public Map<String, List<String>> getConfigList() {
        return configList;
    }

    public void setConfigList(Map<String, List<String>> configList) {
        this.configList = configList;
    }
}
