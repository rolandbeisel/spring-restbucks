package de.odrotbohm.restbucks.engine;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import de.odrotbohm.restbucks.order.Order;
import de.odrotbohm.restbucks.order.Order.OrderPaid;
import de.odrotbohm.restbucks.order.Orders;
import de.odrotbohm.restbucks.order.OrderTestUtils;
import de.odrotbohm.restbucks.payment.CreditCardNumber;
import de.odrotbohm.restbucks.payment.PaymentService;
import io.namastack.outbox.OutboxPayloadSerializer;
import io.namastack.outbox.OutboxRecord;
import io.namastack.outbox.OutboxRecordRepository;
import io.namastack.outbox.OutboxRecordStatus;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@SpringBootTest(properties = {
		"namastack.outbox.polling.fixed.interval=50ms",
		"namastack.outbox.retry.policy=fixed",
		"namastack.outbox.retry.fixed.delay=100ms",
		"restbucks.engine.processing-time=0ms"
})
class OutboxConsistencyIntegrationTest {

	@Autowired Orders orders;
	@Autowired PaymentService payments;
	@Autowired JdbcTemplate jdbc;
	@Autowired PlatformTransactionManager transactionManager;
	@MockitoSpyBean OutboxRecordRepository records;
	@MockitoSpyBean OutboxPayloadSerializer serializer;

	@Test
	void rollsBackOrderChangesAndBothScheduledDeliveriesTogether() {

		var order = orders.save(OrderTestUtils.createOrder());
		var key = "order:" + order.getId().id();
		var transaction = new TransactionTemplate(transactionManager);

		transaction.executeWithoutResult(status -> {
			orders.markPaid(orders.findById(order.getId()).orElseThrow());
			assertThat(deliveryCount(key)).isEqualTo(2);
			status.setRollbackOnly();
		});

		assertThat(orders.findById(order.getId()).orElseThrow().getStatus())
				.isEqualTo(Order.Status.PAYMENT_EXPECTED);
		assertThat(deliveryCount(key)).isZero();
	}

	@Test
	void rollsBackPaymentWhenEventSerializationFails() {

		var order = orders.save(OrderTestUtils.createOrder());
		var key = "order:" + order.getId().id();
		doThrow(new IllegalStateException("Event serialization unavailable"))
				.when(serializer).serialize(any(OrderPaid.class));

		assertThatThrownBy(() -> payments.pay(order, CreditCardNumber.of("1234123412341234")))
				.hasRootCauseInstanceOf(IllegalStateException.class)
				.hasRootCauseMessage("Event serialization unavailable");

		assertThat(orders.findById(order.getId()).orElseThrow().getStatus())
				.isEqualTo(Order.Status.PAYMENT_EXPECTED);
		assertThat(payments.getPaymentFor(order)).isEmpty();
		assertThat(deliveryCount(key)).isZero();
	}

	@Test
	void retriesCompletionAfterOnceCommitWithoutRepeatingPreparation() {

		var order = orders.save(OrderTestUtils.createOrder());
		var key = "order:" + order.getId().id();
		var markersBefore = engineMarkerCount();
		var failNextCompletion = new AtomicBoolean(true);
		var completionAttempts = new AtomicInteger();
		var committedStateAtFailure = new AtomicReference<Order.Status>();
		var committedMarkersAtFailure = new AtomicLong();

		doAnswer(invocation -> {
			OutboxRecord<?> record = invocation.getArgument(0);
			if (record.getKey().equals(key) && record.getHandlerId().equals("engine.prepare-order")
					&& record.getStatus() == OutboxRecordStatus.COMPLETED) {
				completionAttempts.incrementAndGet();
				if (failNextCompletion.getAndSet(false)) {
					committedStateAtFailure.set(orders.findById(order.getId()).orElseThrow().getStatus());
					committedMarkersAtFailure.set(engineMarkerCount());
					throw new IllegalStateException("Outbox completion unavailable after Once commit");
				}
			}
			return invocation.callRealMethod();
		}).when(records).updateProcessingState(any());

		payments.pay(order, CreditCardNumber.of("1234123412341234"));

		await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
			assertThat(jdbc.queryForObject(
					"select count(*) from outbox_record where record_key = ? and status = 'COMPLETED'",
					Long.class, key)).isEqualTo(2);
		});

		assertThat(completionAttempts.get()).isGreaterThanOrEqualTo(2);
		assertThat(committedStateAtFailure.get()).isEqualTo(Order.Status.READY);
		assertThat(committedMarkersAtFailure.get()).isEqualTo(markersBefore + 1);
		assertThat(engineMarkerCount()).isEqualTo(markersBefore + 1);
		assertThat(orders.findById(order.getId()).orElseThrow().getStatus()).isEqualTo(Order.Status.READY);
		assertThat(jdbc.queryForObject(
				"select failure_count from outbox_record where record_key = ? and handler_id = 'engine.prepare-order'",
				Integer.class, key)).isZero();
	}

	private long deliveryCount(String key) {
		return jdbc.queryForObject("select count(*) from outbox_record where record_key = ?", Long.class, key);
	}

	private long engineMarkerCount() {
		return jdbc.queryForObject(
				"select count(*) from once_marker where scope = 'engine.prepare-order'", Long.class);
	}
}
