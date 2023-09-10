package org.disertatie.dbsync;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.github.javafaker.Faker;

// @SpringBootApplication
public class DbsyncApplication {
	public static void main(String[] args) {
    Faker faker = new Faker();
    System.out.println(faker.name().firstName());
    System.out.println(faker.name().lastName());
    System.out.println(faker.number().numberBetween(1,80));
    System.out.println(faker.number().digits(9));
    
    // SpringApplication.run(DbsyncApplication.class, args);
	}
}
