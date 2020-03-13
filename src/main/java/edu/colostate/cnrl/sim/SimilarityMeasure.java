package edu.colostate.cnrl.sim;

import com.google.gson.Gson;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.*;
import java.util.stream.Stream;


public class SimilarityMeasure {

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

    @Procedure("cnrl.similarityMeasure")
    public Stream<NodeListResult> similarityMeasure(@Name("similarityScore") double similarityScore,
                                                    @Name("redFlagMultiple") double redFlagMultiple,
                                                    @Name("queryLabel") String queryLabel,
                                                    @Name("queryFocusLabel") String queryFocusLabel){

        this.redFlagMultiple = redFlagMultiple;
        this.queryLabel = Label.label(queryLabel);
        this.queryFocusLabel = Label.label(queryFocusLabel);

        Common common = new Common(this.db, this.queryLabel, this.queryFocusLabel, NeighbourRelType,
                ActivityNodeType, this.redFlagMultiple, RedFlag, configFileName);
        this.configList = common.readConfigurations();
        common.setConfigList(configList);

        QueryGraphResult queryGraph = common.initializeQueryGraph();

        List<List<Node>> resultSet = graphSimilarityMeasure(common, queryGraph, similarityScore, redFlagMultiple);

        return resultSet.stream().map(nodeList -> new NodeListResult(nodeList));
//        return queryGraph.getAllNodes().stream().map(node -> new NodeResult(node));
    }



    private List<List<Node>> graphSimilarityMeasure(Common common, QueryGraphResult queryGraphResult, double similarityScore, double redFlagMultiple) {

        List<Node> queryGraph = queryGraphResult.getAllNodes();

        List<List<Node>> matchedGraphs = new ArrayList<>();

        //get all query focus set
        ResourceIterator<Node> qfNodes = db.findNodes(queryFocusLabel);
        double totalWeight = common.getTotalWeight(queryGraph, redFlagMultiple);

        while (qfNodes.hasNext()) {
            Node qfNode = qfNodes.next();
            MatchedGraph matchedGraph = common.searchSimilarGraphs(common, qfNode, queryGraph);

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
