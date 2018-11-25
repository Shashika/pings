package edu.colostate.cnrl.sim;

import org.neo4j.graphdb.Node;

import java.util.List;

public class QueryGraphResult {

    private List<Node> allNodes;
    private List<Node> activityNodes;

    public List<Node> getAllNodes() {
        return allNodes;
    }

    public void setAllNodes(List<Node> allNodes) {
        this.allNodes = allNodes;
    }

    public List<Node> getActivityNodes() {
        return activityNodes;
    }

    public void setActivityNodes(List<Node> activityNodes) {
        this.activityNodes = activityNodes;
    }
}
