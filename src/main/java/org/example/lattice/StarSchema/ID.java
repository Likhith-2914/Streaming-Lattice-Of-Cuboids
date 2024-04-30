package org.example.lattice.StarSchema;

import lombok.*;

import java.security.SecureRandom;

@Getter
@Setter
public class ID {

    private String name;
    private String type;
    private Boolean lattice;

    public ID (String name, String type, Boolean lattice) {
        this.name = name;
        this.type = type;
        this.lattice = lattice;
    }
}
