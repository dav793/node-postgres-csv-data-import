DROP TABLE Ticket;

DROP TABLE Violation;

DROP TABLE Vehicle_Description;

CREATE TABLE Vehicle_Description(
    PlateID  varchar(10) PRIMARY KEY,
    VehicleColor varchar(10),
    VehicleMake varchar(10),
    PlateType varchar (10)
);

CREATE TABLE Violation (
    ViolationCode smallint PRIMARY KEY,
    Description varchar(200)
);

CREATE TABLE Ticket(
    SummonsNumber bigint,
    PlateID varchar(10),
    ViolationCode smallint,
    Year varchar(5),

    CONSTRAINT ticket_id PRIMARY KEY (SummonsNumber, Year)

    --CONSTRAINT vehicle_ticket FOREIGN KEY (PlateID)
       --REFERENCES Vehicle_Description (PlateID),

    --CONSTRAINT violation_type FOREIGN KEY (ViolationCode)
      --  REFERENCES Violation (ViolationCode)
);

SET citus.shard_count = 3;

SET citus.shard_replication_factor = 1;

CREATE INDEX violation_type ON Violation (ViolationCode);
CREATE INDEX vehicle_type ON Vehicle_Description (PlateID);
CREATE INDEX ticket_type ON Ticket (SummonsNumber);

SELECT CREATE_REFERENCE_TABLE('Violation');

SELECT CREATE_REFERENCE_TABLE('Vehicle_Description');

SET citus.shard_replication_factor = 2;

SELECT CREATE_DISTRIBUTED_TABLE('Ticket', 'year');

SET citus.shard_replication_factor = 1;

