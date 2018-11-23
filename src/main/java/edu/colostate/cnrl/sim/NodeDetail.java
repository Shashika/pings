package edu.colostate.cnrl.sim;

import org.neo4j.graphdb.Node;

public class NodeDetail {

    private Node node;
    private boolean isNotLeafNode;

    public NodeDetail(Node node) {
        this.node = node;
    }

    public NodeDetail(Node node, boolean isNotLeafNode) {
        this.node = node;
        this.isNotLeafNode = isNotLeafNode;
    }

    public Node getNode() {
        return node;
    }

    public void setNode(Node node) {
        this.node = node;
    }

    public boolean isNotLeafNode() {
        return isNotLeafNode;
    }

    public void setNotLeafNode(boolean notLeafNode) {
        isNotLeafNode = notLeafNode;
    }
}
