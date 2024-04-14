CREATE SEQUENCE usage_type_seq START 1;

CREATE TABLE UsageType (
    UsageTypeID INT DEFAULT NEXTVAL('usage_type_seq') PRIMARY KEY,
    IsPayAtLocation BOOLEAN,
    IsMembershipRequired BOOLEAN,
    IsAccessKeyRequired BOOLEAN,
    Title VARCHAR(255)
);

CREATE SEQUENCE status_type_seq START 1;

CREATE TABLE StatusType (
    StatusTypeID INT DEFAULT NEXTVAL('status_type_seq') PRIMARY KEY,
    IsOperational BOOLEAN,
    IsUserSelectable BOOLEAN,
    Title VARCHAR(255)
);

CREATE SEQUENCE level_seq START 1;

CREATE TABLE Level (
    LevelID INT DEFAULT NEXTVAL('level_seq') PRIMARY KEY,
    Comments VARCHAR(255),
    IsFastChargeCapable BOOLEAN,
    Title VARCHAR(255)
);

CREATE SEQUENCE current_type_seq START 1;

CREATE TABLE CurrentType (
    CurrentTypeID INT DEFAULT NEXTVAL('current_type_seq') PRIMARY KEY,
    Description VARCHAR(255),
    Title VARCHAR(255)
);

CREATE SEQUENCE submission_status_seq START 1;

CREATE TABLE SubmissionStatus (
    SubmissionStatusTypeID INT DEFAULT NEXTVAL('submission_status_seq') PRIMARY KEY,
    IsLive BOOLEAN,
    Title VARCHAR(255)
);

CREATE SEQUENCE country_seq START 1;

CREATE TABLE Country (
    CountryID INT DEFAULT NEXTVAL('country_seq') PRIMARY KEY,
    ISOCode VARCHAR(2),
    ContinentCode VARCHAR(2),
    Title VARCHAR(255)
);

CREATE SEQUENCE address_info_seq START 1;

CREATE TABLE AddressInfo (
    AddressID INT DEFAULT NEXTVAL('address_info_seq') PRIMARY KEY,
    Title VARCHAR(255),
    AddressLine1 VARCHAR(255),
    AddressLine2 VARCHAR(255),
    Town VARCHAR(255),
    StateOrProvince VARCHAR(255),
    Postcode VARCHAR(255),
    CountryID INT,
    Latitude DECIMAL(18, 15),
    Longitude DECIMAL(18, 15),
    ContactTelephone1 VARCHAR(20),
    ContactTelephone2 VARCHAR(20),
    ContactEmail VARCHAR(255),
    AccessComments TEXT,
    RelatedURL VARCHAR(255),
    Distance DECIMAL(10, 2),
    DistanceUnit INT,
    FOREIGN KEY (CountryID) REFERENCES Country(CountryID)
);

CREATE SEQUENCE connection_seq START 1;

CREATE TABLE Connection (
    ConnectionID INT DEFAULT NEXTVAL('connection_seq') PRIMARY KEY,
    StatusTypeID INT,
    LevelID INT,
    CurrentTypeID INT,
    Quantity INT,
    PowerKW INT,
    Comments VARCHAR(255),
    FOREIGN KEY (StatusTypeID) REFERENCES StatusType(StatusTypeID),
    FOREIGN KEY (LevelID) REFERENCES Level(LevelID),
    FOREIGN KEY (CurrentTypeID) REFERENCES CurrentType(CurrentTypeID)
);

CREATE SEQUENCE open_charge_map_seq START 1;

CREATE TABLE OpenChargeMap (
    ID INT DEFAULT NEXTVAL('open_charge_map_seq') PRIMARY KEY,
    UUID VARCHAR(36),
    AddressID INT,
    ConnectionID INT,
    UsageTypeID INT,
    IsRecentlyVerified BOOLEAN,
    DateLastVerified TIMESTAMP,
    DateCreated TIMESTAMP,
    SubmissionStatusTypeID INT,
    FOREIGN KEY (UsageTypeID) REFERENCES UsageType(UsageTypeID),
    FOREIGN KEY (SubmissionStatusTypeID) REFERENCES SubmissionStatus(SubmissionStatusTypeID),
    FOREIGN KEY (AddressID) REFERENCES AddressInfo(AddressID),
    FOREIGN KEY (ConnectionID) REFERENCES Connection(ConnectionID)
);

ALTER TABLE Country
ADD CONSTRAINT unique_country_title UNIQUE (Title);

ALTER TABLE CurrentType
ADD CONSTRAINT unique_current_type_title UNIQUE (Title);

ALTER TABLE SubmissionStatus
ADD CONSTRAINT unique_submission_status_title UNIQUE (Title);

ALTER TABLE Level
ADD CONSTRAINT unique_level_title UNIQUE (Title);

ALTER TABLE StatusType
ADD CONSTRAINT unique_status_type_title UNIQUE (Title);

