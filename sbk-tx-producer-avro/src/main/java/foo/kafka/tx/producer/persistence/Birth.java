package foo.kafka.tx.producer.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;

@Getter
@Setter
@Entity
@Table(name = "births")
public class Birth {
    @NotNull(message = "id must not be null")
    @Id
    @Column(name = "reg_id", nullable = false)
    private Long id;

    @NotNull(message = "name must not be null")
    @Size(max = 100)
    @Column(name = "name", length = 100)
    private String name;

    @NotNull(message = "dob must not be null")
    @Column(name = "dob")
    private LocalDate dob;

    @NotNull(message = "town must not be null")
    @Size(max = 50)
    @Column(name = "town", length = 50)
    private String town;

    @Column(name = "tm")
    private Instant registrationTime = Instant.now();

    @Column(name = "weight", precision = 3, scale = 1)
    private BigDecimal weight;

}