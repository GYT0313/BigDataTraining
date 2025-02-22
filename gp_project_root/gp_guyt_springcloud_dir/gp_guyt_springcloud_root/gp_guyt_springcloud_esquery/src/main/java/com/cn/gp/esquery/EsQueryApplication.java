package com.cn.gp.esquery;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.netflix.eureka.server.EnableEurekaServer;
import org.springframework.cloud.netflix.feign.EnableFeignClients;


@SpringBootApplication
@EnableEurekaServer
@EnableDiscoveryClient
@EnableFeignClients
public class EsQueryApplication {
    public static void main(String[] args) {
        SpringApplication.run(EsQueryApplication.class, args);
    }
}
