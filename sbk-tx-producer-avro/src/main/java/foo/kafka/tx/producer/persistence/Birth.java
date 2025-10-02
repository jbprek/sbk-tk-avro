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
    @Id
    @Column(name = "reg_id", nullable = false)
    private Long id;

    @Size(max = 100)
    @NotNull
    @Column(name = "name", nullable = false, length = 100)
    private String name;

    @NotNull
    @Column(name = "dob", nullable = false)
    private LocalDate dob;

    @Size(max = 50)
    @NotNull
    @Column(name = "town", nullable = false, length = 50)
    private String town;

    @Column(name = "reg_tm")
    private Instant registrationTime = Instant.now();

    @Column(name = "weight", precision = 3, scale = 1)
    private BigDecimal weight;

}