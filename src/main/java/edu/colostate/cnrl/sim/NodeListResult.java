package edu.colostate.cnrl.sim;

import org.neo4j.graphdb.Node;

import java.util.List;

public class NodeListResult  {

    public List<Node> matchedList;

    public NodeListResult(List<Node> list) {
        this.matchedList = list;
    }
}
