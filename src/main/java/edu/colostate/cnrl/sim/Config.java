package edu.colostate.cnrl.sim;

public class Config {

    private String type;
    private String label;
    private String property;

    public Config() {
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {

        if(label.equals("null")){
            this.label = null;
        }
        else{
            this.label = label;
        }
    }

    public String getProperty() {
        return property;
    }

    public void setProperty(String property) {

        if(property.equals("null")){
            this.property = null;
        }
        else{
            this.property = property;
        }
    }
}
