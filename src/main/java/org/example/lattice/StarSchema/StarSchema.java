package org.example.lattice.StarSchema;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class StarSchema {

    List<Dimension> dimensionList;
    List<Fact> factList;
}
