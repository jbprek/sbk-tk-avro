package foo.kafka.tx.consumer.persistence;

import jakarta.persistence.*;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Past;
import jakarta.validation.constraints.Size;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDate;

@Getter
@Setter
@Entity
@Table(name = "birth_stats", uniqueConstraints = {
    @UniqueConstraint(name = "unique_name_dob", columnNames = {"dob", "town"})
})
public class BirthStatEntry {
    @Id
    @Column(name = "reg_id", nullable = false)
    private Long id;

    @NotNull
    @Past
    @Column(name = "dob")
    private LocalDate dob;

    @NotNull
    @Size(max = 50)
    @Column(name = "town", length = 50)
    private String town;

}