package com.example.conduit.common.model;

import io.azam.ulidj.MonotonicULID;
import jakarta.persistence.Id;
import jakarta.persistence.MappedSuperclass;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.Serializable;

@MappedSuperclass
@Data
@EqualsAndHashCode
@ToString
public class KeyedEntity implements Serializable {

    private static final MonotonicULID monotonicULID = new MonotonicULID();

    @Id
    private String id = monotonicULID.generate();
}
