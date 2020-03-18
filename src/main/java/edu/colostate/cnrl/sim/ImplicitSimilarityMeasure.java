package edu.colostate.cnrl.sim;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;


public class ImplicitSimilarityMeasure {

    @Context
    public GraphDatabaseAPI db;

    private static Label queryFocusLabel = null;
    private static double redFlagMultiple = 0;
    private static String entity = null;
    private static String identifier = null;
    private static String identifierName = null;
    private static String relationShipToFocus = null;
    private static Map<String, List<String>> configList = null;
    private static String NeighbourRelType = "NeighbourRelType";
    private static String ActivityNodeType = "ActivityNodeType";
    private static String Identifier = "Identifier";
    private static String RedFlag = "RedFlag";
    private static String configFileName = "/conf_ukcrime.json";

    @Procedure("cnrl.ISimilarityMeasure")
    public Stream<NodeListResult> iSimilarityMeasure(@Name("entity") String entity,
                                                @Name("identifier") String identifier,
                                                @Name("identifierName") String identifierName,
                                                @Name("relationShipToFocus") String relationShipToFocus,
                                                @Name("redFlagMultiple") double redFlagMultiple,
                                                @Name("similarityScore") double similarityScore){

        this.queryFocusLabel = Label.label(entity);
        this.redFlagMultiple = redFlagMultiple;
        this.entity = entity;
        this.identifier = identifier;
        this.identifierName = identifierName;
        this.relationShipToFocus = relationShipToFocus;


        Common common = new Common(this.db, this.entity, this.identifier, this.identifierName, this.relationShipToFocus,
                NeighbourRelType, ActivityNodeType, Identifier, this.redFlagMultiple, RedFlag, configFileName);

        this.configList = common.readConfigurations();
        common.setConfigList(configList);

        QueryGraphResult queryGraph = common.initializeImplicitQueryGraph();

        List<List<Node>> resultSet = implicitGraphSimilarityMeasure(common, queryGraph, similarityScore, redFlagMultiple);

        return resultSet.stream().map(nodeList -> new NodeListResult(nodeList));
//        return queryGraph.getAllNodes().stream().map(node -> new NodeResult(node));

    }

    private List<List<Node>> implicitGraphSimilarityMeasure(Common common, QueryGraphResult queryGraphResult, double similarityScore, double redFlagMultiple) {

        List<Node> queryGraph = queryGraphResult.getAllNodes();

        List<List<Node>> matchedGraphs = new ArrayList<>();

        //get all query focus set
        ResourceIterator<Node> qfNodes = db.findNodes(queryFocusLabel);
        double totalWeight = common.getTotalWeight(queryGraph, redFlagMultiple);

        while (qfNodes.hasNext()) {
            Node qfNode = qfNodes.next();
            MatchedGraph matchedGraph = common.searchImplicitSimilarGraphs(qfNode, queryGraph);

            if(matchedGraph!= null){

                double score = matchedGraph.getMatchScore()/totalWeight;

                if(score >= similarityScore){
                    matchedGraphs.add(matchedGraph.getAllNodes());
                }
            }
        }
        return matchedGraphs;
    }
}
