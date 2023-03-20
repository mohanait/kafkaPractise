package com.projectsync.kafka.orderproducer.customserializers;

import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Order {

    private String customerName;
    private String product;
    private int quantity;
}
