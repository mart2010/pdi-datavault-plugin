/*  usefule command in psql */

--to login as 'am'  on database 'target_am'
psql -d target_am -U am
 


--to view all table in all schema
\dt *.*




select *
from information_schema.schemata;

select current_user;

select current_schema;


select *
from public.h1;




DROP TABLE H1;
CREATE TABLE H1 (
  H1_ID       integer PRIMARY KEY,
  INT1_K        integer NOT NULL,
  AUDIT_DTS   date,
  AUDIT_REC   varchar(45),
  CONSTRAINT H1_UNIQUE UNIQUE (INT1_K)
) ;



DROP TABLE H2;
CREATE TABLE H2 (
  H2_ID       integer PRIMARY KEY,
  INT2_K        integer NOT NULL,
  AUDIT_DTS   date,
  AUDIT_REC   varchar(45),
  CONSTRAINT H2_UNIQUE UNIQUE (INT2_K)
) ;


DROP TABLE S2_1;
CREATE TABLE S2_1 (
  H2_ID integer,
  string1 varchar(1),
  string3 varchar(3),
  ValidFrom date,
  ValidTo date,
  AUDIT_DTS   date,
  AUDIT_REC   varchar(45),
  PRIMARY KEY (H2_ID,ValidFrom),
  FOREIGN KEY (H2_ID) REFERENCES H2 (H2_ID)
);

DROP TABLE S2_2;
CREATE TABLE S2_2 (
  H2_ID integer,
  string1 varchar(1),
  string3 varchar(3),
  AUDIT_DTS   date,
  AUDIT_REC   varchar(45),
  PRIMARY KEY (H2_ID),
  FOREIGN KEY (H2_ID) REFERENCES H2 (H2_ID)
);


DROP TABLE H3;
CREATE TABLE H3 (
  H3_ID       integer PRIMARY KEY,
  INT3_K        integer NOT NULL,
  AUDIT_DTS   date,
  AUDIT_REC   varchar(45),
  CONSTRAINT H3_UNIQUE UNIQUE (INT3_K)
) ;


DROP TABLE H4;
CREATE TABLE H4 (
  H4_ID       integer PRIMARY KEY,
  INT1_K        integer NOT NULL,
  INT2_K        integer NOT NULL,
  INT3_K        integer NOT NULL,
  AUDIT_DTS   date,
  AUDIT_REC   varchar(45),
  CONSTRAINT H4_UNIQUE UNIQUE (INT1_K,INT2_K,INT3_K)
) ;


DROP TABLE H10;
CREATE TABLE H10 (
  H10_ID       integer PRIMARY KEY,
  INT10_K        integer NOT NULL,
  AUDIT_DTS   date,
  AUDIT_REC   varchar(45),
  CONSTRAINT H10_UNIQUE UNIQUE (INT10_K)
) ;



DROP TABLE L123;
CREATE TABLE L123 (
  L123_ID integer PRIMARY KEY,
  H1_ID integer NOT NULL,
  H2_ID integer NOT NULL,
  H3_ID integer NOT NULL,
  AUDIT_DTS   date,
  AUDIT_REC   varchar(45),
  CONSTRAINT LINK123_UNIQUE UNIQUE(H1_ID,H2_ID,H3_ID),
  FOREIGN KEY (H1_ID) REFERENCES H1 (H1_ID) ,
  FOREIGN KEY (H2_ID) REFERENCES H2 (H2_ID),
  FOREIGN KEY (H3_ID) REFERENCES H3 (H3_ID)
);


DROP TABLE S123_1;
CREATE TABLE S123_1 (
  L123_ID integer,
  string5 varchar(5),
  dec1_1 numeric(2,1),
  ValidFrom date,
  ValidTo date,
  AUDIT_DTS   date,
  AUDIT_REC   varchar(45),
  PRIMARY KEY (L123_ID,ValidFrom),
  FOREIGN KEY (L123_ID) REFERENCES L123 (L123_ID)
);

DROP TABLE S123_2;
CREATE TABLE S123_2 (
  L123_ID integer NOT NULL,
  int_3 integer,
  daterandom date,
  ValidFrom date NOT NULL,
  AUDIT_DTS   date,
  AUDIT_REC   varchar(45),
  PRIMARY KEY (L123_ID,ValidFrom),
  FOREIGN KEY (L123_ID) REFERENCES L123 (L123_ID)
);



DROP TABLE L14;
CREATE TABLE L14 (
  L14_ID integer NOT NULL,
  H1_ID integer NOT NULL,
  H4_ID integer NOT NULL,
  AUDIT_DTS   date,
  AUDIT_REC   varchar(45),
  PRIMARY KEY (L14_ID),
  CONSTRAINT LINK14_UNIQUE UNIQUE (H1_ID,H4_ID),
  FOREIGN KEY (H1_ID) REFERENCES H1 (H1_ID),
  FOREIGN KEY (H4_ID) REFERENCES H4 (H4_ID)
);

create sequence h1_seq;
create sequence h2_seq;
create sequence h3_seq;
create sequence h4_seq;
create sequence h10_seq;
create sequence L14_seq;
create sequence L123_seq;


select * from H1;
select * from H2;
select * from H3;
select * from H4;
select count(*) from H4;

select * from L123;
select count(*) from L123;
select * from L14_ai;
select count(*) from L14;

select * from S2_1;
select * from S2_2;
select * from S123_1
ORDER BY 1,4;
select * from S123_2
ORDER BY 1,4;

delete from S123_1;
delete from L123;
delete from S2_1;
delete from S2_2;
delete from L14;
delete from H4;
delete from H3;
delete from H2;
delete from H1;



