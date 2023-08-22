package com.baeldung.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

public class CarItemProcessor implements ItemProcessor<Car, Car> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CarItemProcessor.class);

    @Override
    public Car process(final Car car) {
        String brand = car.getBrand().toUpperCase();
        String origin = car.getModel().toUpperCase();

        Car transformedCar = new Car(brand, origin);
        LOGGER.info("Converting ( {} ) into ( {} )", car, transformedCar);

        return transformedCar;
    }

}
