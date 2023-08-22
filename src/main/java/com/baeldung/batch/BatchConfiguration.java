package com.baeldung.batch;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class BatchConfiguration {

    @Value("${file.input}")
    private String fileInput;

    @Bean
    public FlatFileItemReader<Car> carItemReader() {
        return new FlatFileItemReaderBuilder<Car>().name("carItemReader")
            .resource(new ClassPathResource(fileInput))
            .delimited()
            .names("brand", "model")
            .fieldSetMapper(new BeanWrapperFieldSetMapper<>() {{
                setTargetType(Car.class);
            }})
            .build();
    }

    @Bean
    public CarItemProcessor carItemProcessor() {
        return new CarItemProcessor();
    }

    @Bean
    public JdbcBatchItemWriter<Car> writer(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Car>().itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
            .sql("INSERT INTO car (brand, model) VALUES (:brand, :model)")
            .dataSource(dataSource)
            .build();
    }

    @Bean
    public Step step1(
        JobRepository jobRepository,
        PlatformTransactionManager transactionManager,
        JdbcBatchItemWriter<Car> writer
    ) {
        return new StepBuilder("step1", jobRepository)
            .<Car, Car>chunk(10, transactionManager)
            .reader(carItemReader())
            .processor(carItemProcessor())
            .writer(writer)
            .build();
    }

    @Bean
    public Job importCarsJob(JobRepository jobRepository, JobCompletionNotificationListener listener, Step step1) {
        return new JobBuilder("importCarsJob", jobRepository)
            .incrementer(new RunIdIncrementer())
            .listener(listener)
            .flow(step1)
            .end()
            .build();
    }

    @Bean
    public TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setCorePoolSize(4);

        return taskExecutor;
    }

}
