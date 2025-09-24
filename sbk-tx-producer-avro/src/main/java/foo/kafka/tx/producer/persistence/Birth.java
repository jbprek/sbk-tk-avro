package foo.kafka.tx.producer.persistence;

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
@Table(name = "births")
public class Birth {
    @Id
    @Column(name = "reg_id", nullable = false)
    private Long id;

    @Size(max = 100)
    @Column(name = "name", length = 100)
    private String name;

    @Column(name = "dob")
    private LocalDate dob;

    @Size(max = 50)
    @Column(name = "town", length = 50)
    private String town;

}