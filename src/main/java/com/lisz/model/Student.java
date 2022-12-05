package com.lisz.model;

import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;
import java.util.Set;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Table(keyspace = "school", name = "student")
public class Student {
    @PartitionKey
    private Long id;
    private String address;
    private int age;
    private String name;
    private int gender;
    private List<String> phone;
    private Set<String> interest;
    private Map<String, String> education;
//    private String email;
}
