package com.example.conduit;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Spins a throwaway Postgres for integration tests. {@code @ServiceConnection} rewires the app's
 * datasource to the container, so Liquibase runs against real Postgres and {@code ddl-auto=validate}
 * checks the JPA mappings against the migrated schema — the Phase 0 "done when".
 */
@TestConfiguration(proxyBeanMethods = false)
public class TestcontainersConfiguration {

    @Bean
    @ServiceConnection
    PostgreSQLContainer<?> postgresContainer() {
        return new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"));
    }
}
