
CREATE TABLE UsageType (
    UsageTypeID INT PRIMARY KEY,
    IsPayAtLocation BOOLEAN,
    IsMembershipRequired BOOLEAN,
    IsAccessKeyRequired BOOLEAN,
    Title VARCHAR(255)
);


CREATE TABLE StatusType (
    StatusTypeID INT PRIMARY KEY,
    IsOperational BOOLEAN,
    IsUserSelectable BOOLEAN,
    Title VARCHAR(255)
);

CREATE TABLE Level (
    LevelID INT PRIMARY KEY,
    Comments VARCHAR(255),
    IsFastChargeCapable BOOLEAN,
    Title VARCHAR(255)
);

CREATE TABLE CurrentType (
    CurrentTypeID INT PRIMARY KEY,
    Description VARCHAR(255),
    Title VARCHAR(255)
);

CREATE TABLE SubmissionStatus (
    SubmissionStatusTypeID INT PRIMARY KEY,
    IsLive BOOLEAN,
    Title VARCHAR(255)
);


CREATE TABLE AddressInfo (
    AddressID INT PRIMARY KEY,
    Title VARCHAR(255),
    AddressLine1 VARCHAR(255),
    AddressLine2 VARCHAR(255),
    Town VARCHAR(255),
    StateOrProvince VARCHAR(255),
    Postcode VARCHAR(255),
    CountryID INT,
    Latitude DECIMAL(10, 8),
    Longitude DECIMAL(11, 8),
    ContactTelephone1 VARCHAR(20),
    ContactTelephone2 VARCHAR(20),
    ContactEmail VARCHAR(255),
    AccessComments VARCHAR(255),
    RelatedURL VARCHAR(255),
    Distance DECIMAL(10, 2),
    DistanceUnit INT
);

CREATE TABLE Connection (
    ConnectionID INT PRIMARY KEY,
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

CREATE TABLE Country (
    CountryID INT PRIMARY KEY,
    ISOCode VARCHAR(2),
    ContinentCode VARCHAR(2),
    Title VARCHAR(255)
);


CREATE TABLE AddressInfo (
    AddressID INT PRIMARY KEY,
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


CREATE TABLE OpenChargeMap (
    ID INT PRIMARY KEY,
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
