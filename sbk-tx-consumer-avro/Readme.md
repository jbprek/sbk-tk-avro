# Transactional consumer with Avro serialization

I want to ensure that the message consumption and database storage are done in a single transaction.
Consume a message of type BirthEvent-> event
Transform event to tpe BirthStatEntry ->entity
Store entity in the database
If Database error occurs
if (error is DataIntegrityViolationException or ConstraintViolationException or some other non transient exception)
do acknowledge and skip the message.
else
do not acknowledge and retry the message.

I want to ensure that the message consumption and database storage are done in a single transaction.
