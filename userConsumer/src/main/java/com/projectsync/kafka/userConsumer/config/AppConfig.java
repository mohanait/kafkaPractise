package com.projectsync.kafka.userConsumer.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;

// https://stackoverflow.com/questions/54690518/spring-kafka-jsondesirialization-messageconversionexception-failed-to-resolve-cl
@Configuration
public class AppConfig {

    @Bean
    public StringJsonMessageConverter jsonConverter() {
        return new StringJsonMessageConverter();
    }
}
