package org.example.lattice.StarSchema;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public class Dimension {

    private String name;
    private ID id;
    private List<Property> propertyList;

    public Dimension() {
        propertyList = new ArrayList<>();
    }

    public void addProperty(Property p) {
        propertyList.add(p);
    }
}
