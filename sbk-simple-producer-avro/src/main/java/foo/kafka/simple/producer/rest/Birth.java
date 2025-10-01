package foo.kafka.simple.producer.rest;

import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.Data;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;

@Data
public class Birth {
    @NotNull(message = "id must not be null")
    private Long id;

    @NotNull(message = "name must not be null")
    @Size(max = 100)
    private String name;

    @NotNull(message = "dob must not be null")
    private LocalDate dob;

    @NotNull(message = "town must not be null")
    @Size(max = 50)
    private String town;

    private Instant registrationTime = Instant.now();

    @Digits(integer = 2, fraction = 1, message = "weight must have up to 3 digits with 1 decimal place")
    private BigDecimal weight;

}