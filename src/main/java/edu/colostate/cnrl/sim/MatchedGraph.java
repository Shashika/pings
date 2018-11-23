package edu.colostate.cnrl.sim;

import org.neo4j.graphdb.Node;

import java.util.List;

public class MatchedGraph {

    private List<Node> matchedGraph;
    private double matchedScore;

    public MatchedGraph(List<Node> matchedGraph, double matchedScore) {
        this.matchedGraph = matchedGraph;
        this.matchedScore = matchedScore;
    }

    public List<Node> getMatchedGraph() {
        return matchedGraph;
    }

    public void setMatchedGraph(List<Node> matchedGraph) {
        this.matchedGraph = matchedGraph;
    }

    public double getMatchedScore() {
        return matchedScore;
    }

    public void setMatchedScore(double matchedScore) {
        this.matchedScore = matchedScore;
    }
}
