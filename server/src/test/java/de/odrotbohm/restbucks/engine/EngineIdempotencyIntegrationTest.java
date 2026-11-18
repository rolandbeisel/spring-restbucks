package de.odrotbohm.restbucks.engine;

import static org.assertj.core.api.Assertions.*;

import de.odrotbohm.restbucks.order.Order;
import de.odrotbohm.restbucks.order.Order.OrderPaid;
import de.odrotbohm.restbucks.order.Order.ProcessingCompleted;
import de.odrotbohm.restbucks.order.Orders;
import de.odrotbohm.restbucks.order.OrderTestUtils;
import io.namastack.outbox.handler.OutboxRecordMetadata;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.event.ApplicationEvents;
import org.springframework.test.context.event.RecordApplicationEvents;

@SpringBootTest(properties = {
		"namastack.outbox.enabled=false",
		"restbucks.engine.processing-time=0ms"
})
@RecordApplicationEvents
class EngineIdempotencyIntegrationTest {

	@Autowired Engine engine;
	@Autowired Orders orders;
	@Autowired JdbcTemplate jdbc;
	@Autowired ApplicationEvents events;
	@Autowired PreparationFailure failure;

	@Test
	void preparesAnOrderOnceAcrossRepeatedDeliveries() {

		var order = orders.save(OrderTestUtils.createPaidOrder());
		var event = new OrderPaid(order.getId(), order.getPrice());
		var recordId = UUID.randomUUID().toString();
		events.clear();

		engine.handleOrderPaidEvent(event, metadata(event, recordId, 0));
		engine.handleOrderPaidEvent(event, metadata(event, recordId, 1));

		assertThat(orders.findById(order.getId()).orElseThrow().getStatus()).isEqualTo(Order.Status.READY);
		assertThat(events.stream(Engine.PreparationStarted.class).count()).isEqualTo(1);
		assertThat(events.stream(Engine.PreparationFinished.class).count()).isEqualTo(1);
	}

	@Test
	void rollsBackTheMarkerAndOrderChangesWhenPreparationFails() {

		var order = orders.save(OrderTestUtils.createPaidOrder());
		var event = new OrderPaid(order.getId(), order.getPrice());
		var recordId = UUID.randomUUID().toString();
		var markersBefore = markerCount();
		failure.failNext.set(true);

		assertThatThrownBy(() -> engine.handleOrderPaidEvent(event, metadata(event, recordId, 0)))
				.isInstanceOf(IllegalStateException.class)
				.hasMessage("Temporary preparation failure");

		assertThat(orders.findById(order.getId()).orElseThrow().getStatus()).isEqualTo(Order.Status.PAID);
		assertThat(markerCount()).isEqualTo(markersBefore);

		engine.handleOrderPaidEvent(event, metadata(event, recordId, 1));

		assertThat(orders.findById(order.getId()).orElseThrow().getStatus()).isEqualTo(Order.Status.READY);
		assertThat(markerCount()).isEqualTo(markersBefore + 1);
	}

	private OutboxRecordMetadata metadata(OrderPaid event, String recordId, int failureCount) {
		return new OutboxRecordMetadata("order:" + event.orderIdentifier().id(), "engine.prepare-order",
				Instant.EPOCH, Map.of(), failureCount, recordId, "default");
	}

	private long markerCount() {
		return jdbc.queryForObject(
				"select count(*) from once_marker where scope = 'engine.prepare-order'", Long.class);
	}

	@TestConfiguration(proxyBeanMethods = false)
	static class Configuration {

		@Bean
		PreparationFailure preparationFailure() {
			return new PreparationFailure();
		}
	}

	static class PreparationFailure {

		final AtomicBoolean failNext = new AtomicBoolean();

		@EventListener
		void on(ProcessingCompleted event) {
			if (failNext.getAndSet(false)) {
				throw new IllegalStateException("Temporary preparation failure");
			}
		}
	}
}
