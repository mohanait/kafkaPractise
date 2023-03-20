package com.projectsync.kafka.orderconsumer.customdeserializers;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Order {

    private String customerName;
    private String product;
    private int quantity;
}
