package edu.colostate.cnrl.sim;

public class Conf {

    private NodeDetails[] nodes;
    private NodeDetails red_flag;
    private String neighbour_relationship_type;
    private String activity_node_type;
    private String identifier_type;

    public NodeDetails[] getNodes() {
        return nodes;
    }

    public void setNodes(NodeDetails[] nodes) {
        this.nodes = nodes;
    }

    public NodeDetails getRed_flag() {
        return red_flag;
    }

    public void setRed_flag(NodeDetails red_flag) {
        this.red_flag = red_flag;
    }

    public String getNeighbour_relationship_type() {
        return neighbour_relationship_type;
    }

    public void setNeighbour_relationship_type(String neighbour_relationship_type) {
        this.neighbour_relationship_type = neighbour_relationship_type;
    }

    public String getActivity_node_type() {
        return activity_node_type;
    }

    public void setActivity_node_type(String activity_node_type) {
        this.activity_node_type = activity_node_type;
    }

    public String getIdentifier_type() {
        return identifier_type;
    }

    public void setIdentifier_type(String identifier_type) {
        this.identifier_type = identifier_type;
    }
}