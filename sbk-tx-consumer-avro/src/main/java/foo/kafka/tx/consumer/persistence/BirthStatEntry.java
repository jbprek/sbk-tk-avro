package foo.kafka.tx.consumer.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.validation.constraints.Size;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDate;

@Getter
@Setter
@Entity
@Table(name = "birth_stats")
public class BirthStatEntry {
    @Id
    @Column(name = "reg_id", nullable = false)
    private Long id;

    @Column(name = "dob")
    private LocalDate dob;

    @Size(max = 50)
    @Column(name = "town", length = 50)
    private String town;

}