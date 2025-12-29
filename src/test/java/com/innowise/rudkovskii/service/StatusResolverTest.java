package com.innowise.rudkovskii.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(MockitoExtension.class)
class StatusResolverTest {

    @Test
    void generateStatus_ShouldReturnSuccess_WhenRandomNumberIsEven() {
        try (MockedStatic<RandomIntegerGenerator> mockedRandom =
                     Mockito.mockStatic(RandomIntegerGenerator.class)) {
            mockedRandom.when(RandomIntegerGenerator::getRandomInteger)
                    .thenReturn(2);

            String result = StatusResolver.generateStatus();

            assertEquals("SUCCESS", result);
            mockedRandom.verify(RandomIntegerGenerator::getRandomInteger);
        }
    }

    @Test
    void generateStatus_ShouldReturnFailed_WhenRandomNumberIsOdd() {
        try (MockedStatic<RandomIntegerGenerator> mockedRandom =
                     Mockito.mockStatic(RandomIntegerGenerator.class)) {
            mockedRandom.when(RandomIntegerGenerator::getRandomInteger)
                    .thenReturn(1);

            String result = StatusResolver.generateStatus();

            assertEquals("FAILED", result);
            mockedRandom.verify(RandomIntegerGenerator::getRandomInteger);
        }
    }

    @Test
    void generateStatus_ShouldReturnSuccess_WhenRandomNumberIsZero() {
        try (MockedStatic<RandomIntegerGenerator> mockedRandom =
                     Mockito.mockStatic(RandomIntegerGenerator.class)) {
            mockedRandom.when(RandomIntegerGenerator::getRandomInteger)
                    .thenReturn(0);

            String result = StatusResolver.generateStatus();

            assertEquals("SUCCESS", result);
        }
    }

    @Test
    void generateStatus_ShouldReturnFailed_WhenRandomNumberIsNegativeOdd() {
        try (MockedStatic<RandomIntegerGenerator> mockedRandom =
                     Mockito.mockStatic(RandomIntegerGenerator.class)) {
            mockedRandom.when(RandomIntegerGenerator::getRandomInteger)
                    .thenReturn(-3);

            String result = StatusResolver.generateStatus();

            assertEquals("FAILED", result);
        }
    }

    @Test
    void generateStatus_ShouldReturnSuccess_WhenRandomNumberIsNegativeEven() {
        try (MockedStatic<RandomIntegerGenerator> mockedRandom =
                     Mockito.mockStatic(RandomIntegerGenerator.class)) {
            mockedRandom.when(RandomIntegerGenerator::getRandomInteger)
                    .thenReturn(-4);

            String result = StatusResolver.generateStatus();

            assertEquals("SUCCESS", result);
        }
    }
}