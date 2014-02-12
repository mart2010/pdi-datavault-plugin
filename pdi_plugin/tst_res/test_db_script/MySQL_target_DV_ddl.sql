
USE `target_am`;

DROP TABLE `H1`;
CREATE TABLE `H1` (
  `H1_ID`       int(11) NOT NULL,
  `INT1_K`        int(5) NOT NULL,
  `AUDIT_DTS`   datetime,
  `AUDIT_REC`   varchar(45),
  PRIMARY KEY (`H1_ID`),
  UNIQUE KEY `H1_UNIQUE` (`INT1_K`)
) ;

DROP TABLE `H2`;
CREATE TABLE `H2` (
  `H2_ID`       int(11) NOT NULL,
  `INT2_K`        int(5) NOT NULL,
  `AUDIT_DTS`   datetime,
  `AUDIT_REC`   varchar(45),
  PRIMARY KEY (`H2_ID`),
  UNIQUE KEY `H2_UNIQUE` (`INT2_K`)
) ;


DROP TABLE `S2_1`;
CREATE TABLE `S2_1` (
  `H2_ID` int(11) NOT NULL,
  `string1` varchar(1),
  `string3` varchar(3),
  `ValidFrom` datetime NOT NULL,
  `ValidTo` datetime,
  `AUDIT_DTS`   datetime,
  `AUDIT_REC`   varchar(45),
  PRIMARY KEY (`H2_ID`,`ValidFrom`),
  CONSTRAINT `fk_S2_1` FOREIGN KEY (`H2_ID`) REFERENCES `H2` (`H2_ID`) ON DELETE NO ACTION ON UPDATE NO ACTION
);

DROP TABLE `S2_2`;
CREATE TABLE `S2_2` (
  `H2_ID` int(11) NOT NULL,
  `string1` varchar(1),
  `string3` varchar(3),
  `AUDIT_DTS`   datetime,
  `AUDIT_REC`   varchar(45),
  PRIMARY KEY (`H2_ID`),
  CONSTRAINT `fk_S2_2` FOREIGN KEY (`H2_ID`) REFERENCES `H2` (`H2_ID`) ON DELETE NO ACTION ON UPDATE NO ACTION
);



DROP TABLE `H3`;
CREATE TABLE `H3` (
  `H3_ID`       int(11) NOT NULL,
  `INT3_K`        int(5) NOT NULL,
  `AUDIT_DTS`   datetime,
  `AUDIT_REC`   varchar(45),
  PRIMARY KEY (`H3_ID`),
  UNIQUE KEY `H3_UNIQUE` (`INT3_K`)
) ;

DROP TABLE `H4`;
CREATE TABLE `H4` (
  `H4_ID`       int(11) NOT NULL,
  `INT1_K`        int(5) NOT NULL,
  `INT2_K`        int(5) NOT NULL,
  `INT3_K`        int(5) NOT NULL,
  `AUDIT_DTS`   datetime,
  `AUDIT_REC`   varchar(45),
  PRIMARY KEY (`H4_ID`),
  UNIQUE KEY `H4_UNIQUE` (`INT1_K`,`INT2_K`,`INT3_K`)
) ;

DROP TABLE `H10`;
CREATE TABLE `H10` (
  `H10_ID`       int(11) NOT NULL,
  `INT10_K`        int(11) NOT NULL,
  `AUDIT_DTS`   datetime,
  `AUDIT_REC`   varchar(45),
  PRIMARY KEY (`H10_ID`),
  UNIQUE KEY `H10_UNIQUE` (`INT10_K`)
) ;


DROP TABLE `L123_ai`;
CREATE TABLE `L123_ai` (
  `L123_ID` int(11) NOT NULL AUTO_INCREMENT,
  `H1_ID` int(11) NOT NULL,
  `H2_ID` int(11) NOT NULL,
  `H3_ID` int(11) NOT NULL,
  `AUDIT_DTS`   datetime,
  `AUDIT_REC`   varchar(45),
  PRIMARY KEY (`L123_ID`),
  UNIQUE KEY `LINK123_UNIQUE` (`H1_ID`,`H2_ID`,`H3_ID`),
  CONSTRAINT `fk_LINK_1` FOREIGN KEY (`H1_ID`) REFERENCES `H1` (`H1_ID`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT `fk_LINK_2` FOREIGN KEY (`H2_ID`) REFERENCES `H2` (`H2_ID`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT `fk_LINK_3` FOREIGN KEY (`H3_ID`) REFERENCES `H3` (`H3_ID`) ON DELETE NO ACTION ON UPDATE NO ACTION
);


DROP TABLE `S123_1`;
CREATE TABLE `S123_1` (
  `L123_ID` int(11) NOT NULL,
  `string5` varchar(5),
  `dec1_1` decimal(2,1),
  `ValidFrom` datetime NOT NULL,
  `ValidTo` datetime,
  `AUDIT_DTS`   datetime,
  `AUDIT_REC`   varchar(45),
  PRIMARY KEY (`L123_ID`,`ValidFrom`),
  CONSTRAINT `fk_S123_1` FOREIGN KEY (`L123_ID`) REFERENCES `L123_ai` (`L123_ID`) ON DELETE NO ACTION ON UPDATE NO ACTION
);

DROP TABLE `S123_2`;
CREATE TABLE `S123_2` (
  `L123_ID` int(11) NOT NULL,
  `int_3` int(3),
  `daterandom` datetime,
  `ValidFrom` datetime NOT NULL,
  `AUDIT_DTS`   datetime,
  `AUDIT_REC`   varchar(45),
  PRIMARY KEY (`L123_ID`,`ValidFrom`),
  CONSTRAINT `fk_S123_2` FOREIGN KEY (`L123_ID`) REFERENCES `L123_ai` (`L123_ID`) ON DELETE NO ACTION ON UPDATE NO ACTION
);



DROP TABLE `L14_ai`;
CREATE TABLE `L14_ai` (
  `L14_ID` int(11) NOT NULL AUTO_INCREMENT,
  `H1_ID` int(11) NOT NULL,
  `H4_ID` int(11) NOT NULL,
  `AUDIT_DTS`   datetime,
  `AUDIT_REC`   varchar(45),
  PRIMARY KEY (`L14_ID`),
  UNIQUE KEY `LINK14_UNIQUE` (`H1_ID`,`H4_ID`),
  CONSTRAINT `fk_LINK14_1` FOREIGN KEY (`H1_ID`) REFERENCES `H1` (`H1_ID`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT `fk_LINK14_2` FOREIGN KEY (`H4_ID`) REFERENCES `H4` (`H4_ID`) ON DELETE NO ACTION ON UPDATE NO ACTION
);


select * from target_am.H1;
select * from target_am.H2;
select count(*) from target_am.H3;
select count(*) from target_am.H4;
select * from target_am.H4;

select * from target_am.L123_ai;
select count(*) from target_am.L123_ai;
select * from target_am.L14_ai;
select count(*) from target_am.L14_ai;

select count(*) from target_am.S2_1;
select * from target_am.S2_1;
select count(*) from target_am.S2_2;
select * from target_am.S2_2;
select count(*) from target_am.S123_1;
select * from target_am.S123_1
ORDER BY 1,4;
select count(*) from target_am.S123_2;

select * from target_am.S123_2
ORDER BY 1,4;

delete from target_am.S123_1;
delete from target_am.L123_ai;
delete from target_am.S2_1;
delete from target_am.S2_2;
delete from target_am.L14_ai;
delete from target_am.H4;
delete from target_am.H3;
delete from target_am.H2;
delete from target_am.H1;


SELECT *
FROM target_am.s2_1 Sat 
WHERE h2_ID IN ( 104,105,106,107,104,105,106)  
AND ValidFrom >= (  SELECT case when max(ValidFrom) is not null then max(ValidFrom) else DATE '1000-01-01' end 
                    FROM s2_1 
                    WHERE h2_ID = Sat.h2_ID AND ValidFrom < DATE '2013-01-01' )