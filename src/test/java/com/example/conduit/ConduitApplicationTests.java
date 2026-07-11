package com.example.conduit;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import(TestcontainersConfiguration.class)
class ConduitApplicationTests {

	/**
	 * Boots the full context against a real Postgres (Testcontainers): Liquibase applies all
	 * changesets, then Hibernate {@code ddl-auto=validate} confirms every entity maps to the
	 * migrated schema. Green = Phase 0 done.
	 */
	@Test
	void contextLoads() {
	}

}
