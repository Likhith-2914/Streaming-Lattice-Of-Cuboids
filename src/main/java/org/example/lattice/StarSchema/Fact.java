package org.example.lattice.StarSchema;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class Fact {
    private String name;
    private List<String> aggregate_functions;

    private void addAggregateFunction(String f) {aggregate_functions.add(f);}
}
