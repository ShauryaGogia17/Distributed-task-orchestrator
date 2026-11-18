package io.taskscheduler.worker;

import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.util.backoff.FixedBackOff;

/** Fail-closed record errors, explicit acknowledgement, and independent maintenance threads. */
@Configuration
public class Infrastructure {
  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
      ConsumerFactory<String, String> consumers,
      WorkerProperties properties,
      @Value("${PYTHON_WORKER_GROUP_ID:${WORKER_GROUP_ID:task-workers}}") String pythonGroup) {
    if (properties.groupId().equals(pythonGroup))
      throw new IllegalArgumentException("Java and Python pending groups must differ");
    ConcurrentKafkaListenerContainerFactory<String, String> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumers);
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
    factory.getContainerProperties().setSyncCommits(true);
    factory.getContainerProperties().setShutdownTimeout(90000);
    factory.getContainerProperties().setPollTimeout(100);
    DefaultErrorHandler errors =
        new DefaultErrorHandler(new FixedBackOff(1000, FixedBackOff.UNLIMITED_ATTEMPTS));
    errors.setAckAfterHandle(false);
    errors.setCommitRecovered(false);
    errors.addRetryableExceptions(IllegalArgumentException.class);
    factory.setCommonErrorHandler(errors);
    return factory;
  }

  @Bean
  public TaskScheduler taskScheduler() {
    ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
    scheduler.setPoolSize(3);
    scheduler.setThreadNamePrefix("java-maintenance-");
    scheduler.setWaitForTasksToCompleteOnShutdown(true);
    scheduler.setAwaitTerminationSeconds(30);
    return scheduler;
  }

  @Bean
  public HealthIndicator javaKafkaHealth(KafkaAdmin configuration) {
    return () -> {
      Map<String, Object> settings = new HashMap<>(configuration.getConfigurationProperties());
      settings.put("default.api.timeout.ms", 3000);
      settings.put("request.timeout.ms", 3000);
      try (AdminClient admin = AdminClient.create(settings)) {
        var topic =
            admin
                .describeTopics(List.of("tasks.pending"))
                .allTopicNames()
                .get(3, TimeUnit.SECONDS)
                .get("tasks.pending");
        boolean ready =
            topic.partitions().size() == 6
                && topic.partitions().stream()
                    .allMatch(
                        partition -> partition.leader() != null && partition.leader().id() >= 0);
        return ready ? Health.up().build() : Health.down().build();
      } catch (Exception e) {
        return Health.down().build();
      }
    };
  }

  @Bean
  public HealthIndicator workerHealth(
      WorkerRuntime runtime, KafkaListenerEndpointRegistry registry) {
    return () ->
        runtime.healthy()
                && !registry.getListenerContainers().isEmpty()
                && registry.getListenerContainers().stream()
                    .allMatch(container -> container.isRunning())
            ? Health.up().build()
            : Health.down().build();
  }
}
