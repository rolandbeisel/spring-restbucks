package de.odrotbohm.restbucks.dashboard;

import static org.assertj.core.api.Assertions.*;

import de.odrotbohm.restbucks.order.Order.OrderIdentifier;
import de.odrotbohm.restbucks.order.Order.OrderPaid;
import io.namastack.outbox.handler.OutboxRecordMetadata;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import org.javamoney.moneta.Money;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

@SpringBootTest(properties = "namastack.outbox.enabled=false")
class DashboardIdempotencyIntegrationTest {

	@Autowired Dashboard dashboard;
	@Autowired JdbcTemplate jdbc;

	@Test
	void countsRepeatedRecordsOnceAndDifferentRecordsForTheSameOrderIndependently() {

		var amount = Money.of(4.20, "EUR");
		var event = new OrderPaid(new OrderIdentifier(UUID.randomUUID()), amount);
		var firstRecordId = UUID.randomUUID().toString();
		var secondRecordId = UUID.randomUUID().toString();
		var markersBefore = jdbc.queryForObject(
				"select count(*) from once_marker where scope = 'dashboard.record-order-revenue'", Long.class);

		dashboard.on(event, metadata(event, firstRecordId, 0));
		dashboard.on(event, metadata(event, firstRecordId, 1));
		dashboard.on(event, metadata(event, secondRecordId, 0));
		dashboard.on(event, metadata(event, secondRecordId, 1));

		assertThat(dashboard.statistics()).containsEntry("revenue", amount.multiply(2));
		assertThat(jdbc.queryForObject(
				"select count(*) from once_marker where scope = 'dashboard.record-order-revenue'", Long.class))
				.isEqualTo(markersBefore + 2);
	}

	private OutboxRecordMetadata metadata(OrderPaid event, String recordId, int failureCount) {
		return new OutboxRecordMetadata("order:" + event.orderIdentifier().id(), "dashboard.record-order-revenue",
				Instant.EPOCH, Map.of(), failureCount, recordId, "default");
	}
}
